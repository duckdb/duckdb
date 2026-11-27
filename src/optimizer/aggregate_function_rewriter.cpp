#include "duckdb/optimizer/aggregate_function_rewriter.hpp"
#include "duckdb/common/types/decimal.hpp"

#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/optimizer/aggregate_rewrite.hpp"
#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_nullability.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

static void RewriteAggregateCallbacks(Optimizer &optimizer, unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &aggregate_op = op->Cast<LogicalAggregate>();
		for (auto &expr : aggregate_op.expressions) {
			auto &aggregate = expr->Cast<BoundAggregateExpression>();
			AggregateRewriteInput input(optimizer, aggregate_op, aggregate);
			auto rewrite = TryDirectAggregateRewrite(input);
			if (!rewrite) {
				continue;
			}
			expr = std::move(rewrite);
		}
	}
	for (auto &child : op->children) {
		RewriteAggregateCallbacks(optimizer, child);
	}
}

class AggregateRewriteRule {
public:
	explicit AggregateRewriteRule(Optimizer &optimizer_p) : optimizer(optimizer_p) {
	}
	virtual ~AggregateRewriteRule() = default;

public:
	virtual bool ShouldSkip(const LogicalAggregate &aggr) const = 0;
	virtual bool ShouldRewrite(const LogicalAggregate &aggr, Expression &expr) {
		return true;
	}
	virtual unique_ptr<Expression> Rewrite(unique_ptr<Expression> &expr, vector<reference<Expression>> &bindings,
	                                       vector<unique_ptr<Expression>> &additional_expressions) = 0;
	virtual unique_ptr<Expression>
	CreateProjectionExpression(const LogicalType &aggr_type, unique_ptr<Expression> aggr_ref,
	                           vector<unique_ptr<Expression>> additional_expressions) = 0;

public:
	Optimizer &optimizer;
	unique_ptr<ExpressionMatcher> matcher;
};

//! The rewrite of one aggregate, as an AggregateRewriteAnalyzer has planned it
struct AnalyzedAggregateRewrite {
	virtual ~AnalyzedAggregateRewrite() = default;

	//! The aggregate that replaces the original aggregate
	unique_ptr<Expression> aggregate;
	//! The helper aggregates of the rewrite. A helper that equals an aggregate of the operator re-uses that aggregate
	vector<unique_ptr<Expression>> helpers;

	//! Whether a projection computes the result of the original aggregate: without one, the rewritten aggregate is
	//! the result
	virtual bool HasProjection() const = 0;
	//! The result of the original aggregate, computed from the rewritten aggregate and the helpers (in the order of
	//! `helpers`)
	virtual unique_ptr<Expression> CreateProjectionExpression(Optimizer &optimizer,
	                                                          unique_ptr<Expression> aggregate_ref,
	                                                          vector<unique_ptr<Expression>> helper_refs) const = 0;
};

//! Plans the rewrite of an aggregate in one step, instead of the matcher and the callbacks of an AggregateRewriteRule
class AggregateRewriteAnalyzer {
public:
	virtual ~AggregateRewriteAnalyzer() = default;

	//! Returns the rewrite of the aggregate, or nullptr if the aggregate stays as it is
	virtual unique_ptr<AnalyzedAggregateRewrite> Analyze(LogicalAggregate &aggr, const Expression &expr) = 0;
};

static unique_ptr<SetTypesMatcher> GetSmallIntegerTypesMatcher() {
	vector<LogicalType> types {LogicalTypeId::TINYINT,  LogicalTypeId::SMALLINT, LogicalTypeId::INTEGER,
	                           LogicalTypeId::BIGINT,   LogicalTypeId::UTINYINT, LogicalTypeId::USMALLINT,
	                           LogicalTypeId::UINTEGER, LogicalTypeId::UBIGINT};
	return make_uniq<SetTypesMatcher>(std::move(types));
}

//! AVG(x) -> SUM(x) / COUNT(x)
class AvgRewriteRule : public AggregateRewriteRule {
public:
	AvgRewriteRule(Optimizer &optimizer, LogicalOperator &root)
	    : AggregateRewriteRule(optimizer), nullability(optimizer.context, root) {
		auto op = make_uniq<AggregateExpressionMatcher>();
		op->function = make_uniq<SpecificFunctionMatcher>("avg");
		op->type = make_uniq<NumericTypeMatcher>();
		op->policy = SetMatcher::Policy::ORDERED;
		auto child_matcher = make_uniq<StableExpressionMatcher>();
		child_matcher->type = make_uniq<NumericTypeMatcher>();
		op->matchers.push_back(std::move(child_matcher));
		matcher = std::move(op);

		auto count = make_uniq<AggregateExpressionMatcher>();
		count->function = make_uniq<ManyFunctionMatcher>(identifier_set_t {"count", "count_star"});
		count->policy = SetMatcher::Policy::SOME;
		count_matcher = std::move(count);
	}

public:
	bool ShouldSkip(const LogicalAggregate &aggr) const override {
		return !aggr.grouping_functions.empty();
	}

	bool ShouldRewrite(const LogicalAggregate &aggr, Expression &expr) override {
		auto &child_type = expr.Cast<BoundAggregateExpression>().GetChildren()[0]->GetReturnType();
		if (child_type.id() == LogicalTypeId::DECIMAL && Decimal::IsWide(DecimalType::GetWidth(child_type))) {
			// the wide avg divides its exact sum before the result becomes a DOUBLE, SUM / COUNT would overflow it
			return false;
		}
		if (aggr.groups.empty()) {
			return true;
		}
		if (optimizer.OptimizerDisabled(OptimizerType::COMMON_AGGREGATE)) {
			return false;
		}
		auto &child = *expr.Cast<BoundAggregateExpression>().GetChildren()[0];
		FunctionBinder binder(optimizer.context);
		vector<unique_ptr<Expression>> sum_args;
		sum_args.push_back(child.Copy());
		auto sum = binder.BindAggregateFunction(
		    GetBuiltinAggregateFunction(optimizer.context, "sum", {child.GetReturnType()}), std::move(sum_args));
		vector<unique_ptr<Expression>> count_args;
		count_args.push_back(child.Copy());
		auto count = binder.BindAggregateFunction(
		    GetBuiltinAggregateFunction(optimizer.context, "count", {child.GetReturnType()}), std::move(count_args));

		D_ASSERT(aggr.children.size() == 1);
		auto is_not_null = [&](const Expression &input) {
			if (input.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				return !input.Cast<BoundConstantExpression>().GetValue().IsNull();
			}
			return nullability.IsNotNull(*aggr.children[0], input);
		};
		const auto can_share_count =
		    !optimizer.OptimizerDisabled(OptimizerType::STATISTICS_PROPAGATION) && is_not_null(child);
		for (auto &other : aggr.expressions) {
			// Identical AVGs already share their native state.
			if (expr.Equals(*other)) {
				continue;
			}
			if (sum->Equals(*other) || count->Equals(*other)) {
				return true;
			}
			if (!can_share_count || other->IsVolatile()) {
				continue;
			}
			vector<reference<Expression>> bindings;
			if (!count_matcher->Match(*other, bindings) && !matcher->Match(*other, bindings)) {
				continue;
			}
			auto &children = other->Cast<BoundAggregateExpression>().GetChildren();
			if (children.empty() || (children.size() == 1 && is_not_null(*children[0]))) {
				return true;
			}
		}
		return false;
	}

	unique_ptr<Expression> Rewrite(unique_ptr<Expression> &expr, vector<reference<Expression>> &bindings,
	                               vector<unique_ptr<Expression>> &additional_expressions) override {
		FunctionBinder function_binder(optimizer.context);

		// Move the child out of AVG(x)
		auto avg_child = std::move(bindings[0].get().Cast<BoundAggregateExpression>().GetChildrenMutable()[0]);

		// Replace AVG(x) with SUM(x)
		auto sum_fun = GetBuiltinAggregateFunction(optimizer.context, "sum", {avg_child->GetReturnType()});
		vector<unique_ptr<Expression>> args;
		args.push_back(std::move(avg_child));
		auto count_arg = args.back()->Copy();
		expr = function_binder.BindAggregateFunction(sum_fun, std::move(args));

		return count_arg;
	}

	unique_ptr<Expression> CreateProjectionExpression(const LogicalType &, unique_ptr<Expression> aggr_ref,
	                                                  vector<unique_ptr<Expression>> additional_expressions) override {
		// SUM(x) / COUNT(x): additional_expressions[0] is the COUNT(x) ref
		return optimizer.BindScalarFunction("/", std::move(aggr_ref), std::move(additional_expressions[0]));
	}

private:
	NotNullExpressionAnalyzer nullability;
	unique_ptr<ExpressionMatcher> count_matcher;
};

//! SUM(x + C) -> SUM(x) + C * COUNT(x)
//! SUM(x - C) -> SUM(x) - C * COUNT(x)
//! SUM(C - x) -> C * COUNT(x) - SUM(x)
class SumRewriteRule : public AggregateRewriteRule {
public:
	explicit SumRewriteRule(Optimizer &optimizer) : AggregateRewriteRule(optimizer) {
		// Match SUM(x + C), SUM(C + x), SUM(x - C) or SUM(C - x) on integer types
		auto op = make_uniq<AggregateExpressionMatcher>();
		op->function = make_uniq<SpecificFunctionMatcher>("sum");
		op->policy = SetMatcher::Policy::UNORDERED;

		auto arithmetic = make_uniq<FunctionExpressionMatcher>();
		arithmetic->function = make_uniq<ManyFunctionMatcher>(identifier_set_t {"+", "-"});
		arithmetic->type = make_uniq<IntegerTypeMatcher>();
		auto child_constant_matcher = make_uniq<ConstantExpressionMatcher>();
		auto child_expression_matcher = make_uniq<StableExpressionMatcher>();
		child_constant_matcher->type = GetSmallIntegerTypesMatcher();
		child_expression_matcher->type = GetSmallIntegerTypesMatcher();
		arithmetic->matchers.push_back(std::move(child_constant_matcher));
		arithmetic->matchers.push_back(std::move(child_expression_matcher));
		arithmetic->policy = SetMatcher::Policy::SOME;
		op->matchers.push_back(std::move(arithmetic));

		matcher = std::move(op);
	}

public:
	bool ShouldSkip(const LogicalAggregate &aggr) const override {
		return !aggr.grouping_functions.empty();
	}

	bool ShouldRewrite(const LogicalAggregate &aggr, Expression &expr) override {
		auto &arithmetic = GetArithmetic(expr);
		// unsigned subtraction commonly underflows and throws, and the rewrite would hide that error
		if (arithmetic.Function().GetName() == "-" && !arithmetic.GetReturnType().IsSigned()) {
			return false;
		}
		if (aggr.groups.empty()) {
			return true;
		}
		if (optimizer.OptimizerDisabled(OptimizerType::COMMON_AGGREGATE)) {
			return false;
		}
		// the added COUNT(x) is extra per-group state, so only rewrite if SUM(x) or COUNT(x) is shared
		auto &argument = *arithmetic.GetChildren()[1 - GetConstantIndex(arithmetic)];
		for (auto &other : aggr.expressions) {
			if (expr.Equals(*other)) {
				continue;
			}
			if (ComputesSumOrCount(*other, argument)) {
				return true;
			}
		}
		return false;
	}

	unique_ptr<Expression> Rewrite(unique_ptr<Expression> &, vector<reference<Expression>> &bindings,
	                               vector<unique_ptr<Expression>> &additional_expressions) override {
		auto &sum = bindings[0].get().Cast<BoundAggregateExpression>();
		auto &arithmetic = bindings[1].get().Cast<BoundFunctionExpression>();
		const auto const_idx = GetConstantIndex(arithmetic);
		const bool is_subtract = arithmetic.Function().GetName() == "-";
		const bool sum_is_left = !is_subtract || const_idx == 1;
		auto const_expr = std::move(arithmetic.GetChildrenMutable()[const_idx]);
		auto main_expr = std::move(arithmetic.GetChildrenMutable()[1 - const_idx]);

		// Turn SUM(x +/- C) into SUM(x) - this destroys the arithmetic expression
		sum.GetChildrenMutable()[0] = main_expr->Copy();

		additional_expressions.push_back(std::move(const_expr));
		additional_expressions.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(is_subtract)));
		additional_expressions.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(sum_is_left)));
		return main_expr;
	}

	unique_ptr<Expression> CreateProjectionExpression(const LogicalType &aggr_type, unique_ptr<Expression> aggr_ref,
	                                                  vector<unique_ptr<Expression>> additional_expressions) override {
		// additional_expressions: the const, whether it is a subtraction, whether SUM(x) is the left operand,
		// the COUNT(x) ref
		auto cast_const =
		    BoundCastExpression::AddCastToType(optimizer.context, std::move(additional_expressions[0]), aggr_type);
		auto is_subtract = additional_expressions[1]->Cast<BoundConstantExpression>().GetValue().GetValue<bool>();
		auto sum_is_left = additional_expressions[2]->Cast<BoundConstantExpression>().GetValue().GetValue<bool>();
		auto cast_count =
		    BoundCastExpression::AddCastToType(optimizer.context, std::move(additional_expressions[3]), aggr_type);
		auto multiply = optimizer.BindScalarFunction("*", std::move(cast_count), std::move(cast_const));
		const Identifier function_name = is_subtract ? "-" : "+";
		if (sum_is_left) {
			return optimizer.BindScalarFunction(function_name, std::move(aggr_ref), std::move(multiply));
		}
		return optimizer.BindScalarFunction(function_name, std::move(multiply), std::move(aggr_ref));
	}

private:
	static idx_t GetConstantIndex(const BoundFunctionExpression &arithmetic) {
		return arithmetic.GetChildren()[0]->GetExpressionType() == ExpressionType::VALUE_CONSTANT ? 0 : 1;
	}

	BoundFunctionExpression &GetArithmetic(Expression &expr) {
		vector<reference<Expression>> bindings;
		if (!matcher->Match(expr, bindings)) {
			throw InternalException("SumRewriteRule: expression does not match SUM(x +/- C)");
		}
		return bindings[1].get().Cast<BoundFunctionExpression>();
	}

	//! Whether expr is SUM(argument), COUNT(argument) or SUM(argument +/- C)
	bool ComputesSumOrCount(Expression &expr, const Expression &argument) {
		vector<reference<Expression>> bindings;
		if (matcher->Match(expr, bindings)) {
			auto &arithmetic = bindings[1].get().Cast<BoundFunctionExpression>();
			return arithmetic.GetChildren()[1 - GetConstantIndex(arithmetic)]->Equals(argument);
		}
		auto &aggregate = expr.Cast<BoundAggregateExpression>();
		if (aggregate.GetFilter() || aggregate.GetOrderBys() ||
		    aggregate.GetAggregateType() != AggregateType::NON_DISTINCT) {
			return false;
		}
		const auto &name = aggregate.Function().GetName();
		auto &children = aggregate.GetChildren();
		return (name == "sum" || name == "count") && children.size() == 1 && children[0]->Equals(argument);
	}
};

//! list(expr ORDER BY expr <sense> <nulls>) => list_sort(list(expr), <sense>, <nulls>)
class BindingMatcher : public ExpressionMatcher {
public:
	explicit BindingMatcher(idx_t binding_idx) : ExpressionMatcher(ExpressionClass::INVALID), binding_idx(binding_idx) {
	}

	bool Match(Expression &expr, vector<reference<Expression>> &bindings) override {
		if (!expr.Equals(bindings[binding_idx])) {
			return false;
		}
		bindings.push_back(expr);
		return true;
	}

	const idx_t binding_idx;
};

class OrderedAggregateMatcher : public ExpressionMatcher {
public:
	OrderedAggregateMatcher() : ExpressionMatcher(ExpressionClass::BOUND_AGGREGATE) {
	}

	bool Match(Expression &expr_p, vector<reference<Expression>> &bindings) override {
		if (!ExpressionMatcher::Match(expr_p, bindings)) {
			return false;
		}
		auto &expr = expr_p.Cast<BoundAggregateExpression>();
		// don't rewrite state-export aggregates - list(x ORDER BY x) EXPORT_STATE would become
		// list_sort(list(x) EXPORT_STATE, ...), which cannot bind list_sort on the AGGREGATE_STATE result
		if (expr.StateExportMode() == AggregateStateExportMode::STATE_EXPORT) {
			return false;
		}
		if (!FunctionMatcher::Match(function, expr.Function().GetName())) {
			return false;
		}
		if (!SetMatcher::Match(matchers, expr.GetChildrenMutable(), bindings, policy)) {
			return false;
		}
		if (!expr.GetOrderBys() && !order_bys.empty()) {
			return false;
		}
		if (order_bys.size() != expr.GetOrderBys()->orders.size()) {
			return false;
		}
		for (idx_t i = 0; i < order_bys.size(); ++i) {
			if (!order_bys[i]->Match(*expr.GetOrderBys()->orders[i].expression, bindings)) {
				return false;
			}
		}
		return true;
	}

	//! The matchers for the child expressions
	vector<unique_ptr<ExpressionMatcher>> matchers;
	//! The function name to match
	unique_ptr<FunctionMatcher> function;
	//! The set matcher matching policy to use
	SetMatcher::Policy policy;
	//! The matchers for the order by expressions
	vector<unique_ptr<ExpressionMatcher>> order_bys;
};

class ListRewriteRule : public AggregateRewriteRule {
public:
	explicit ListRewriteRule(Optimizer &optimizer) : AggregateRewriteRule(optimizer) {
		auto op = make_uniq<OrderedAggregateMatcher>();
		op->function = make_uniq<SpecificFunctionMatcher>("list");
		op->policy = SetMatcher::Policy::ORDERED;
		op->matchers.push_back(make_uniq<StableExpressionMatcher>());
		op->order_bys.emplace_back(make_uniq<BindingMatcher>(idx_t(1)));

		matcher = std::move(op);
	}

	bool ShouldSkip(const LogicalAggregate &aggr) const override {
		return false;
	}

	unique_ptr<Expression> Rewrite(unique_ptr<Expression> &expr, vector<reference<Expression>> &bindings,
	                               vector<unique_ptr<Expression>> &additional_expressions) override;

	unique_ptr<Expression> CreateProjectionExpression(const LogicalType &, unique_ptr<Expression> aggr_ref,
	                                                  vector<unique_ptr<Expression>> addnl) override {
		return optimizer.BindScalarFunction("list_sort", std::move(aggr_ref), std::move(addnl[0]), std::move(addnl[1]));
	}
};

unique_ptr<Expression> ListRewriteRule::Rewrite(unique_ptr<Expression> &expr, vector<reference<Expression>> &bindings,
                                                vector<unique_ptr<Expression>> &additional_expressions) {
	auto &aggr = bindings[0].get().Cast<BoundAggregateExpression>();

	auto &order_bys = aggr.GetOrderBysMutable();
	auto &order_by = order_bys->orders[0];

	auto sense = make_uniq<BoundConstantExpression>(EnumUtil::ToChars(order_by.type));
	auto nulls = make_uniq<BoundConstantExpression>(EnumUtil::ToChars(order_by.null_order));

	additional_expressions.emplace_back(std::move(sense));
	additional_expressions.emplace_back(std::move(nulls));

	order_bys.reset();

	return nullptr;
}

//! f(CASE WHEN c THEN x END) -> f(x) FILTER (WHERE c), so that the state is only updated for the matching rows and
//! aggregates that share a condition evaluate it once per chunk. The rewritten forms are the kinds below.
//!
//! Where the aggregate returns a row for an empty input (no groups, or a grouping set without columns), the ELSE 0
//! forms are wrapped in CASE WHEN count(*) > 0 THEN ... END: the sum over no rows is NULL.
//!
//! The CASE only evaluates x for the matching rows, whereas the FILTER form evaluates it for every row. Hence x must
//! not be able to throw and must not be volatile.
enum class CaseToFilterKind : uint8_t {
	//! f(CASE WHEN c THEN x END) -> f(x) FILTER (WHERE c)
	//! for sum / count / avg / min / max / bool_and / bool_or: both are NULL (0 for count) without a matching row.
	//! A DISTINCT stays on the aggregate: the distinct values of the CASE are the distinct x of the matching rows and
	//! a NULL, which these aggregates ignore. An existing FILTER (WHERE p) becomes FILTER (WHERE p AND c)
	FILTERED_AGGREGATE,
	//! count(CASE WHEN c THEN k END) -> count(*) FILTER (WHERE c)
	//! for a non-NULL constant k. Not under DISTINCT: count(DISTINCT CASE WHEN c THEN k END) is 0 or 1, and is
	//! rewritten as a FILTERED_AGGREGATE to count(DISTINCT k) FILTER (WHERE c)
	FILTERED_COUNT_STAR,
	//! sum(CASE WHEN c THEN x ELSE 0 END) -> coalesce(sum(x) FILTER (WHERE c), 0)
	//! if x is provably not NULL
	COALESCED_SUM,
	//! sum(CASE WHEN c THEN x ELSE 0 END) -> CASE WHEN count(*) > count(*) FILTER (WHERE c)
	//!                                            THEN coalesce(sum(x) FILTER (WHERE c), 0)
	//!                                            ELSE sum(x) FILTER (WHERE c) END
	//! if x is nullable: a group in which every row matches and every x is NULL is NULL (a sum over NULLs), any other
	//! group without a non-NULL matching x holds a row that contributed the ELSE 0
	NULLABLE_SUM,
	//! sum(CASE WHEN c THEN k ELSE 0 END) -> count(*) FILTER (WHERE c) * k
	//! for an integer constant k
	SCALED_COUNT
};

//! The rewrite of one conditional aggregate: its kind, and what the projection above the aggregate needs
struct CaseToFilterRewrite : public AnalyzedAggregateRewrite {
	CaseToFilterRewrite(CaseToFilterKind kind_p, LogicalType result_type_p)
	    : kind(kind_p), result_type(std::move(result_type_p)) {
	}

	CaseToFilterKind kind;
	//! The type of the original aggregate
	LogicalType result_type;
	//! The constant of the projection, typed as the sum: the 0 of coalesce(.., 0) or the factor k of the count
	Value constant;
	//! The helper count(*) FILTER (WHERE c) of NULLABLE_SUM: the number of matching rows
	optional_idx matching_count;
	//! The helper count(*): the number of rows, for NULLABLE_SUM and for the other ELSE 0 kinds if the aggregate
	//! returns a row for an empty input
	optional_idx row_count;

	bool HasProjection() const override {
		return kind != CaseToFilterKind::FILTERED_AGGREGATE && kind != CaseToFilterKind::FILTERED_COUNT_STAR;
	}

	unique_ptr<Expression> CreateProjectionExpression(Optimizer &optimizer, unique_ptr<Expression> aggregate_ref,
	                                                  vector<unique_ptr<Expression>> helper_refs) const override {
		unique_ptr<Expression> result;
		switch (kind) {
		case CaseToFilterKind::COALESCED_SUM:
		case CaseToFilterKind::NULLABLE_SUM: {
			// coalesce(sum(x) FILTER (WHERE c), 0)
			auto coalesce = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, result_type);
			coalesce->GetChildrenMutable().push_back(aggregate_ref->Copy());
			coalesce->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(constant));
			if (kind == CaseToFilterKind::NULLABLE_SUM) {
				// only a group with a row that did not match holds the ELSE 0: without one the sum is the result
				auto has_else_rows = BoundComparisonExpression::Create(
				    ExpressionType::COMPARE_GREATERTHAN, std::move(helper_refs[row_count.GetIndex()]),
				    std::move(helper_refs[matching_count.GetIndex()]));
				return make_uniq<BoundCaseExpression>(std::move(has_else_rows), std::move(coalesce),
				                                      std::move(aggregate_ref));
			}
			result = std::move(coalesce);
			break;
		}
		case CaseToFilterKind::SCALED_COUNT:
			// count(*) FILTER (WHERE c) * k
			result = BoundCastExpression::AddCastToType(optimizer.context, std::move(aggregate_ref), result_type);
			if (constant != Value::Numeric(result_type, 1)) {
				result =
				    optimizer.BindScalarFunction("*", std::move(result), make_uniq<BoundConstantExpression>(constant));
				result = BoundCastExpression::AddCastToType(optimizer.context, std::move(result), result_type);
			}
			break;
		default:
			throw InternalException("CaseToFilterRewrite: the filtered aggregate is the result");
		}
		if (!row_count.IsValid()) {
			return result;
		}
		// the sum over an empty input is NULL
		auto has_rows = BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN,
		                                                  std::move(helper_refs[row_count.GetIndex()]),
		                                                  make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
		return make_uniq<BoundCaseExpression>(std::move(has_rows), std::move(result),
		                                      make_uniq<BoundConstantExpression>(Value(result_type)));
	}
};

class CaseToFilterRule : public AggregateRewriteAnalyzer {
public:
	CaseToFilterRule(Optimizer &optimizer_p, LogicalOperator &root, bool use_statistics_p)
	    : optimizer(optimizer_p), nullability(optimizer_p.context, root),
	      use_statistics(use_statistics_p && !optimizer_p.OptimizerDisabled(OptimizerType::STATISTICS_PROPAGATION)) {
	}

private:
	//! The parts of f(CASE WHEN c THEN x [ELSE 0] END), an aggregate that passed the preconditions of the rule
	struct ConditionalAggregate {
		optional_ptr<const BoundAggregateExpression> aggregate;
		//! c
		optional_ptr<const Expression> condition;
		//! x
		optional_ptr<const Expression> value;
		//! p of an existing FILTER (WHERE p)
		optional_ptr<const Expression> filter;
		//! Whether the CASE has an ELSE 0 (under sum) instead of an ELSE NULL
		bool else_zero = false;

		const string &FunctionName() const {
			return aggregate->Function().GetName().GetIdentifierName();
		}
	};

private:
	static bool IsRewritableFunction(const string &name) {
		static constexpr const char *FUNCTIONS[] = {"sum", "count", "avg", "min", "max", "bool_and", "bool_or"};
		for (auto &function_name : FUNCTIONS) {
			if (name == function_name) {
				return true;
			}
		}
		return false;
	}

	static bool IsNonNullConstant(const Expression &expr) {
		return expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		       !expr.Cast<BoundConstantExpression>().GetValue().IsNull();
	}

	static bool IsZero(const Value &value) {
		if (value.IsNull() || !value.type().IsNumeric()) {
			return false;
		}
		return value == Value::Numeric(value.type(), 0);
	}

	//! Whether the expression is an aggregate over a single-branch CASE that passes the preconditions of the rule
	static bool MatchConditionalAggregate(const Expression &expr, ConditionalAggregate &result) {
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		auto &aggr = expr.Cast<BoundAggregateExpression>();
		if (aggr.StateExportMode() == AggregateStateExportMode::STATE_EXPORT || aggr.GetOrderBys() ||
		    aggr.GetChildren().size() != 1) {
			return false;
		}
		if (aggr.GetFilter()) {
			// the rewritten aggregate has the filter `p AND c`, which evaluates p or c for a part of the rows only
			auto &filter = *aggr.GetFilter();
			if (filter.IsVolatile() || !filter.IsConsistent() || filter.CanThrow() || filter.HasSubquery()) {
				return false;
			}
		}
		auto &name = aggr.Function().GetName().GetIdentifierName();
		if (!IsRewritableFunction(name)) {
			return false;
		}
		auto &child = *aggr.GetChildren()[0];
		if (child.GetExpressionClass() != ExpressionClass::BOUND_CASE) {
			return false;
		}
		auto &case_expr = child.Cast<BoundCaseExpression>();
		if (case_expr.CaseChecks().size() != 1) {
			return false;
		}
		auto &when_expr = *case_expr.CaseChecks()[0].when_expr;
		auto &then_expr = *case_expr.CaseChecks()[0].then_expr;
		auto &else_expr = case_expr.Else();
		if (when_expr.IsVolatile() || !when_expr.IsConsistent() || when_expr.HasSubquery() || when_expr.IsAggregate() ||
		    when_expr.IsWindow()) {
			return false;
		}
		// the CASE evaluates the THEN branch for the matching rows only, the FILTER form for every row
		if (then_expr.CanThrow() || then_expr.IsVolatile() || then_expr.HasSubquery()) {
			return false;
		}
		// min / max over strings or nested types update their state faster from the vector of the CASE than from a
		// slice of the argument
		if ((name == "min" || name == "max") && !TypeIsConstantSize(then_expr.GetReturnType().InternalType())) {
			return false;
		}
		const auto is_sum = name == "sum";
		// the sum of the distinct values adds them in the order of the hash table that holds them, which is another
		// order once the values of the rows that do not match are no longer part of it
		if ((is_sum || name == "avg") && aggr.IsDistinct() && then_expr.GetReturnType().IsFloating()) {
			return false;
		}
		// sum and avg add value * count for a constant input vector instead of adding the value count times, which
		// is another floating point number. A constant THEN branch is such a vector in every chunk: the filtered sum
		// would never be the sum of the CASE form, so that it keeps its CASE. A column is not covered by this: it
		// only is a constant vector where the scan or a join hands one out (a constant or RLE segment, the one-row
		// side of a cross join), and there every sum over it adds the product - with a user-written FILTER, too
		if ((is_sum || name == "avg") && then_expr.GetReturnType().IsFloating() &&
		    then_expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			return false;
		}
		// the binder casts the branches to the type of the CASE, so that f(x) binds like f(CASE ...)
		if (then_expr.GetReturnType() != child.GetReturnType()) {
			return false;
		}
		if (else_expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return false;
		}
		auto &else_value = else_expr.Cast<BoundConstantExpression>().GetValue();
		if (!else_value.IsNull()) {
			// the ELSE 0 forms count the rows of the group: these counts are not those of the rows that pass an
			// existing filter, and the 0 is one of the distinct values
			if (!is_sum || !IsZero(else_value) || aggr.GetFilter() || aggr.IsDistinct()) {
				return false;
			}
		}
		result.aggregate = aggr;
		result.condition = when_expr;
		result.value = then_expr;
		result.filter = aggr.GetFilter().get();
		result.else_zero = !else_value.IsNull();
		return true;
	}

	//! Whether the function returns a value whenever its arguments are values. The default NULL handling only states
	//! that a NULL argument gives NULL, not the converse (struct_extract of a NULL field, list_extract past the end)
	static bool PreservesNotNull(const BoundFunctionExpression &function) {
		if (function.Function().GetNullHandling() != FunctionNullHandling::DEFAULT_NULL_HANDLING) {
			return false;
		}
		auto &name = function.Function().GetName().GetIdentifierName();
		return function.IsOperator() && (name == "+" || name == "-" || name == "*");
	}

	//! Whether the expression is provably not NULL at the output of the operator
	bool IsNotNull(LogicalOperator &input, const Expression &expr) {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::BOUND_CONSTANT:
			return IsNonNullConstant(expr);
		case ExpressionClass::BOUND_COLUMN_REF:
			return use_statistics && nullability.IsNotNull(input, expr);
		case ExpressionClass::BOUND_FUNCTION: {
			auto &function = expr.Cast<BoundFunctionExpression>();
			if (BoundCastExpression::IsCast(expr)) {
				// a cast between numeric types returns a value or throws
				auto &source = BoundCastExpression::Child(function);
				return !BoundCastExpression::IsTryCast(function) && source.GetReturnType().IsNumeric() &&
				       expr.GetReturnType().IsNumeric() && IsNotNull(input, source);
			}
			if (!PreservesNotNull(function) || function.GetChildren().empty()) {
				return false;
			}
			for (auto &child : function.GetChildren()) {
				if (!IsNotNull(input, *child)) {
					return false;
				}
			}
			return true;
		}
		default:
			return false;
		}
	}

	//! The form that the aggregate is rewritten to
	CaseToFilterKind GetKind(LogicalAggregate &aggr, const ConditionalAggregate &input) {
		auto &value = *input.value;
		if (!input.else_zero) {
			if (input.FunctionName() == "count" && IsNonNullConstant(value) && !input.aggregate->IsDistinct()) {
				return CaseToFilterKind::FILTERED_COUNT_STAR;
			}
			return CaseToFilterKind::FILTERED_AGGREGATE;
		}
		// k * count(*) is only the exact sum for an integer k
		const auto sum_type = input.aggregate->GetReturnType().id();
		if (IsNonNullConstant(value) && value.GetReturnType().IsIntegral() &&
		    (sum_type == LogicalTypeId::HUGEINT || sum_type == LogicalTypeId::UHUGEINT)) {
			return CaseToFilterKind::SCALED_COUNT;
		}
		if (IsNotNull(*aggr.children[0], value)) {
			return CaseToFilterKind::COALESCED_SUM;
		}
		return CaseToFilterKind::NULLABLE_SUM;
	}

	//! Binds count(*), with an optional filter
	unique_ptr<Expression> BindCountStar(unique_ptr<Expression> filter) const {
		FunctionBinder function_binder(optimizer.context);
		auto count_fun = GetBuiltinAggregateFunction(optimizer.context, CountFun::Name, {});
		return function_binder.BindAggregateFunction(count_fun, {}, std::move(filter), AggregateType::NON_DISTINCT);
	}

	//! The filter of the rewritten aggregate: c, or `p AND c` for an aggregate with a FILTER (WHERE p). A row passes
	//! it if p and c are both true, which are the rows that the CASE aggregate reads a value for
	static unique_ptr<Expression> GetFilter(const ConditionalAggregate &input) {
		if (!input.filter) {
			return input.condition->Copy();
		}
		return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, input.filter->Copy(),
		                                             input.condition->Copy());
	}

	//! Binds the filtered aggregate that replaces the conditional aggregate, or returns nullptr if it cannot replace it
	unique_ptr<Expression> BindFiltered(const ConditionalAggregate &input, CaseToFilterKind kind) const {
		auto &aggr = *input.aggregate;
		unique_ptr<Expression> result;
		if (kind == CaseToFilterKind::FILTERED_COUNT_STAR || kind == CaseToFilterKind::SCALED_COUNT) {
			result = BindCountStar(GetFilter(input));
			if (result->GetReturnType() != LogicalType::BIGINT) {
				return nullptr;
			}
		} else {
			auto function = TryGetBuiltinAggregateFunction(optimizer.context, aggr.Function().GetName(),
			                                               {input.value->GetReturnType()});
			if (!function) {
				return nullptr;
			}
			FunctionBinder function_binder(optimizer.context);
			vector<unique_ptr<Expression>> children;
			children.push_back(input.value->Copy());
			result = function_binder.BindAggregateFunction(function, std::move(children), GetFilter(input),
			                                               aggr.GetAggregateType());
			if (result->GetReturnType() != aggr.GetReturnType()) {
				return nullptr;
			}
		}
		result->SetAlias(aggr.GetAlias());
		return result;
	}

	//! Whether the aggregate returns a row for an empty input
	static bool HasEmptyGroupingSet(const LogicalAggregate &aggr) {
		if (aggr.groups.empty()) {
			return true;
		}
		for (auto &grouping_set : aggr.grouping_sets) {
			if (grouping_set.empty()) {
				return true;
			}
		}
		return false;
	}

public:
	unique_ptr<AnalyzedAggregateRewrite> Analyze(LogicalAggregate &aggr, const Expression &expr) override {
		ConditionalAggregate input;
		if (!MatchConditionalAggregate(expr, input)) {
			return nullptr;
		}
		const auto kind = GetKind(aggr, input);
		auto result = make_uniq<CaseToFilterRewrite>(kind, expr.GetReturnType());
		if (result->HasProjection()) {
			const auto constant = kind == CaseToFilterKind::SCALED_COUNT
			                          ? input.value->Cast<BoundConstantExpression>().GetValue()
			                          : Value::INTEGER(0);
			auto typed_constant = constant.DefaultTryCastAs(result->result_type);
			if (!typed_constant) {
				return nullptr;
			}
			result->constant = std::move(*typed_constant);
		}
		result->aggregate = BindFiltered(input, kind);
		if (!result->aggregate) {
			return nullptr;
		}
		if (kind == CaseToFilterKind::NULLABLE_SUM) {
			// count(*) > count(*) FILTER (WHERE c): a row of the group took the ELSE 0
			result->matching_count = optional_idx(result->helpers.size());
			result->helpers.push_back(BindCountStar(input.condition->Copy()));
		}
		if (kind == CaseToFilterKind::NULLABLE_SUM || (result->HasProjection() && HasEmptyGroupingSet(aggr))) {
			// count(*) > 0: the aggregate has seen a row
			result->row_count = optional_idx(result->helpers.size());
			result->helpers.push_back(BindCountStar(nullptr));
		}
		return std::move(result);
	}

private:
	Optimizer &optimizer;
	NotNullExpressionAnalyzer nullability;
	//! Whether a column may be proven NOT NULL: the proof reads the statistics of the scanned tables, which the
	//! optimizer does not use for a plan with a data-modifying CTE (the rows change while the query runs)
	const bool use_statistics;
};

//! Internal LogicalOperatorVisitor that does the actual rewriting
class AggregateFunctionRewriterInternal : public LogicalOperatorVisitor {
public:
	AggregateFunctionRewriterInternal(Optimizer &optimizer, AggregateRewriteRule &rule)
	    : optimizer(optimizer), rule(rule) {
	}
	AggregateFunctionRewriterInternal(Optimizer &optimizer, AggregateRewriteAnalyzer &analyzer)
	    : optimizer(optimizer), analyzer(analyzer) {
	}

private:
	AggregateFunctionRewriterInternal(Optimizer &optimizer, optional_ptr<AggregateRewriteRule> rule,
	                                  optional_ptr<AggregateRewriteAnalyzer> analyzer)
	    : optimizer(optimizer), rule(rule), analyzer(analyzer) {
	}

	struct RewriteInfo {
		idx_t count_idx;
		vector<unique_ptr<Expression>> additional_expressions;
		//! The rewrite that an analyzer has planned, and the indexes of its helper aggregates
		unique_ptr<AnalyzedAggregateRewrite> analyzed;
		vector<idx_t> helper_indexes;
	};

public:
	void Optimize(unique_ptr<LogicalOperator> &op) {
		if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			RewriteAggregates(op);
		}
		VisitOperator(*op);
	}

	void VisitOperator(LogicalOperator &op) override {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT:
		case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			AggregateFunctionRewriterInternal rewriter(optimizer, rule, analyzer);
			rewriter.StandardVisitOperator(op);
			return;
		}
		default:
			break;
		}
		StandardVisitOperator(op);
	}

private:
	void StandardVisitOperator(LogicalOperator &op) {
		for (auto &child : op.children) {
			Optimize(child);
		}
		if (!aggregate_map.empty()) {
			VisitOperatorExpressions(op);
		}
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *) override {
		const auto entry = aggregate_map.find(expr.Binding());
		if (entry != aggregate_map.end()) {
			return expr.WithBinding(entry->second);
		}
		return nullptr;
	}

	//! Replaces the aggregate by the rewrite that the analyzer returns for it (if any) and adds the helper aggregates
	bool RewriteAnalyzed(LogicalAggregate &aggr, idx_t aggr_idx, RewriteInfo &rewrite_info) {
		rewrite_info.analyzed = analyzer->Analyze(aggr, *aggr.expressions[aggr_idx]);
		if (!rewrite_info.analyzed) {
			return false;
		}
		auto &analyzed = *rewrite_info.analyzed;
		aggr.expressions[aggr_idx] = std::move(analyzed.aggregate);
		for (auto &helper : analyzed.helpers) {
			idx_t helper_idx;
			for (helper_idx = 0; helper_idx < aggr.expressions.size(); helper_idx++) {
				if (aggr.expressions[helper_idx]->Equals(*helper)) {
					break;
				}
			}
			if (helper_idx == aggr.expressions.size()) {
				aggr.expressions.push_back(std::move(helper));
			}
			rewrite_info.helper_indexes.push_back(helper_idx);
		}
		return true;
	}

	void RewriteAggregates(unique_ptr<LogicalOperator> &op) {
		auto &aggr = op->Cast<LogicalAggregate>();
		if (rule && rule->ShouldSkip(aggr)) {
			return;
		}
		const idx_t aggr_count = aggr.expressions.size();

		unordered_map<idx_t, RewriteInfo> rewrites;
		bool needs_projection = false;
		for (idx_t i = 0; i < aggr_count; ++i) {
			RewriteInfo rewrite_info;
			if (analyzer) {
				if (RewriteAnalyzed(aggr, i, rewrite_info)) {
					needs_projection = needs_projection || rewrite_info.analyzed->HasProjection();
					rewrites.emplace(i, std::move(rewrite_info));
				}
				continue;
			}
			auto &expr = aggr.expressions[i];
			vector<reference<Expression>> bindings;
			if (!rule->matcher->Match(*expr, bindings)) {
				continue;
			}
			if (!rule->ShouldRewrite(aggr, *expr)) {
				continue;
			}

			auto count_arg = rule->Rewrite(expr, bindings, rewrite_info.additional_expressions);

			// Add COUNT([x]) to the aggregate list - the count set holds both the unary count and count_star
			FunctionBinder function_binder(optimizer.context);
			vector<unique_ptr<Expression>> count_args;
			vector<LogicalType> count_arg_types;
			if (count_arg) {
				count_arg_types.push_back(count_arg->GetReturnType());
				count_args.push_back(std::move(count_arg));
			}
			auto count_fun = GetBuiltinAggregateFunction(optimizer.context, CountFun::Name, count_arg_types);
			auto count_aggr = function_binder.BindAggregateFunction(count_fun, std::move(count_args), nullptr,
			                                                        AggregateType::NON_DISTINCT);

			rewrite_info.count_idx = aggr.expressions.size();
			rewrites.emplace(i, std::move(rewrite_info));
			aggr.expressions.push_back(std::move(count_aggr));
			needs_projection = true;
		}

		if (!needs_projection) {
			// nothing was rewritten, or every rewritten aggregate is the result as it is
			return;
		}

		// Push a projection that re-computes the original result expressions
		auto proj_index = optimizer.binder.GenerateTableIndex();
		const auto group_count = aggr.groups.size();
		vector<unique_ptr<Expression>> projection_expressions;

		// Pass group bindings through and register them in the aggregate_map
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			ColumnBinding aggregate_binding(aggr.group_index, ProjectionIndex(group_idx));
			aggregate_map[aggregate_binding] = ColumnBinding(proj_index, ProjectionIndex(group_idx));
			auto group_ref =
			    make_uniq<BoundColumnRefExpression>(aggr.groups[group_idx]->GetReturnType(), aggregate_binding);
			projection_expressions.push_back(std::move(group_ref));
		}

		for (idx_t i = 0; i < aggr_count; i++) {
			ColumnBinding aggregate_binding(aggr.aggregate_index, ProjectionIndex(i));
			aggregate_map[aggregate_binding] = ColumnBinding(proj_index, ProjectionIndex(group_count + i));
			auto &aggr_type = aggr.expressions[i]->GetReturnType();
			auto aggr_ref = make_uniq<BoundColumnRefExpression>(aggr_type, aggregate_binding);

			const auto rewrite_entry = rewrites.find(i);
			if (rewrite_entry == rewrites.end()) {
				// Not rewritten — pass through as-is
				projection_expressions.push_back(std::move(aggr_ref));
				continue;
			}

			auto &rewrite_info = rewrite_entry->second;
			if (rewrite_info.analyzed) {
				if (!rewrite_info.analyzed->HasProjection()) {
					// The rewritten aggregate is the result
					projection_expressions.push_back(std::move(aggr_ref));
					continue;
				}
				vector<unique_ptr<Expression>> helper_refs;
				for (auto &helper_idx : rewrite_info.helper_indexes) {
					ColumnBinding helper_binding(aggr.aggregate_index, ProjectionIndex(helper_idx));
					helper_refs.push_back(make_uniq<BoundColumnRefExpression>(
					    aggr.expressions[helper_idx]->GetReturnType(), helper_binding));
				}
				projection_expressions.push_back(rewrite_info.analyzed->CreateProjectionExpression(
				    optimizer, std::move(aggr_ref), std::move(helper_refs)));
				continue;
			}
			ColumnBinding count_binding(aggr.aggregate_index, ProjectionIndex(rewrite_info.count_idx));
			auto count_ref = make_uniq<BoundColumnRefExpression>(
			    aggr.expressions[rewrite_info.count_idx]->GetReturnType(), count_binding);

			rewrite_info.additional_expressions.push_back(std::move(count_ref));
			auto final_result = rule->CreateProjectionExpression(aggr_type, std::move(aggr_ref),
			                                                     std::move(rewrite_info.additional_expressions));
			projection_expressions.push_back(std::move(final_result));
		}

		// Pass grouping-function bindings through after the original aggregates. Any helper aggregates added by the
		// rewrite remain private to this projection.
		for (idx_t grouping_idx = 0; grouping_idx < aggr.grouping_functions.size(); grouping_idx++) {
			ColumnBinding grouping_binding(aggr.groupings_index, ProjectionIndex(grouping_idx));
			aggregate_map[grouping_binding] =
			    ColumnBinding(proj_index, ProjectionIndex(group_count + aggr_count + grouping_idx));
			projection_expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, grouping_binding));
		}

		auto proj = make_uniq<LogicalProjection>(proj_index, std::move(projection_expressions));
		if (op->has_estimated_cardinality) {
			proj->SetEstimatedCardinality(op->estimated_cardinality);
		}
		proj->children.push_back(std::move(op));
		op = std::move(proj);
	}

private:
	Optimizer &optimizer;
	//! The rewrites are either those of a rule or those that an analyzer plans
	optional_ptr<AggregateRewriteRule> rule;
	optional_ptr<AggregateRewriteAnalyzer> analyzer;
	column_binding_map_t<ColumnBinding> aggregate_map;
};

AggregateFunctionRewriter::AggregateFunctionRewriter(Optimizer &optimizer) : optimizer(optimizer) {
}

AggregateFunctionRewriter::~AggregateFunctionRewriter() {
}

void AggregateFunctionRewriter::Optimize(unique_ptr<LogicalOperator> &op) {
	RewriteAggregateCallbacks(optimizer, op);
	vector<unique_ptr<AggregateRewriteRule>> rules;
	rules.push_back(make_uniq<AvgRewriteRule>(optimizer, *op));
	rules.push_back(make_uniq<SumRewriteRule>(optimizer));
	rules.push_back(make_uniq<ListRewriteRule>(optimizer));
	// Run each rule as an independent pass so that transformations by an earlier rule affect later rules
	for (auto &rule : rules) {
		AggregateFunctionRewriterInternal rewriter(optimizer, *rule);
		rewriter.Optimize(op);
	}
}

void AggregateFunctionRewriter::RewriteCaseToFilter(unique_ptr<LogicalOperator> &op, bool use_statistics) {
	CaseToFilterRule rule(optimizer, *op, use_statistics);
	AggregateFunctionRewriterInternal rewriter(optimizer, rule);
	rewriter.Optimize(op);
}

} // namespace duckdb
