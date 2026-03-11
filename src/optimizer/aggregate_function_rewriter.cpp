#include "duckdb/optimizer/aggregate_function_rewriter.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

class AggregateRewriteRule {
public:
	explicit AggregateRewriteRule(Optimizer &optimizer_p) : optimizer(optimizer_p) {
	}
	virtual ~AggregateRewriteRule() = default;

public:
	virtual bool ShouldSkip(const LogicalAggregate &aggr) const = 0;
	virtual unique_ptr<Expression> Rewrite(unique_ptr<Expression> &expr, vector<reference<Expression>> &bindings,
	                                       vector<unique_ptr<Expression>> &additional_expressions) = 0;
	virtual unique_ptr<Expression>
	CreateProjectionExpression(const LogicalType &aggr_type, unique_ptr<Expression> aggr_ref,
	                           vector<unique_ptr<Expression>> additional_expressions) = 0;

public:
	Optimizer &optimizer;
	unique_ptr<ExpressionMatcher> matcher;
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
	explicit AvgRewriteRule(Optimizer &optimizer) : AggregateRewriteRule(optimizer) {
		auto op = make_uniq<AggregateExpressionMatcher>();
		op->function = make_uniq<SpecificFunctionMatcher>("avg");
		op->type = make_uniq<NumericTypeMatcher>();
		op->policy = SetMatcher::Policy::ORDERED;
		auto child_matcher = make_uniq<StableExpressionMatcher>();
		child_matcher->type = make_uniq<NumericTypeMatcher>();
		op->matchers.push_back(std::move(child_matcher));
		matcher = std::move(op);
	}

public:
	bool ShouldSkip(const LogicalAggregate &aggr) const override {
		return !aggr.grouping_functions.empty() || aggr.grouping_sets.size() > 1;
	}

	unique_ptr<Expression> Rewrite(unique_ptr<Expression> &expr, vector<reference<Expression>> &bindings,
	                               vector<unique_ptr<Expression>> &additional_expressions) override {
		auto &catalog = Catalog::GetSystemCatalog(optimizer.context);
		FunctionBinder function_binder(optimizer.context);

		// Move the child out of AVG(x)
		auto avg_child = std::move(bindings[0].get().Cast<BoundAggregateExpression>().children[0]);

		// Replace AVG(x) with SUM(x)
		auto &sum_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(optimizer.context, DEFAULT_SCHEMA, "sum");
		const auto sum_fun = sum_entry.functions.GetFunctionByArguments(optimizer.context, {avg_child->return_type});
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
};

//! SUM(x + C) -> SUM(x) + C * COUNT(x)
class SumRewriteRule : public AggregateRewriteRule {
public:
	explicit SumRewriteRule(Optimizer &optimizer) : AggregateRewriteRule(optimizer) {
		// Match SUM(x + C) or SUM(C + x) on integer types
		auto op = make_uniq<AggregateExpressionMatcher>();
		op->function = make_uniq<SpecificFunctionMatcher>("sum");
		op->policy = SetMatcher::Policy::UNORDERED;

		auto arithmetic = make_uniq<FunctionExpressionMatcher>();
		arithmetic->function = make_uniq<SpecificFunctionMatcher>("+");
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
		// This can probably be relaxed to be the same check as AvgRewriteRule,
		// but behaviour (for now) is the same as the old SumRewriterOptimizer
		return !aggr.groups.empty();
	}

	unique_ptr<Expression> Rewrite(unique_ptr<Expression> &, vector<reference<Expression>> &bindings,
	                               vector<unique_ptr<Expression>> &additional_expressions) override {
		auto &sum = bindings[0].get().Cast<BoundAggregateExpression>();
		auto &addition = bindings[1].get().Cast<BoundFunctionExpression>();
		idx_t const_idx = addition.children[0]->GetExpressionType() == ExpressionType::VALUE_CONSTANT ? 0 : 1;
		auto const_expr = std::move(addition.children[const_idx]);
		auto main_expr = std::move(addition.children[1 - const_idx]);

		// Turn SUM(x + C) into SUM(x)
		sum.children[0] = main_expr->Copy();

		additional_expressions.push_back(std::move(const_expr));
		return main_expr;
	}

	unique_ptr<Expression> CreateProjectionExpression(const LogicalType &aggr_type, unique_ptr<Expression> aggr_ref,
	                                                  vector<unique_ptr<Expression>> additional_expressions) override {
		// SUM(x) + C * COUNT(x): additional_expressions[0] is the const, additional_expressions[1] is COUNT(x) ref
		auto cast_const =
		    BoundCastExpression::AddCastToType(optimizer.context, std::move(additional_expressions[0]), aggr_type);
		auto cast_count =
		    BoundCastExpression::AddCastToType(optimizer.context, std::move(additional_expressions[1]), aggr_type);
		auto multiply = optimizer.BindScalarFunction("*", std::move(cast_count), std::move(cast_const));
		return optimizer.BindScalarFunction("+", std::move(aggr_ref), std::move(multiply));
	}
};

//! Internal LogicalOperatorVisitor that does the actual rewriting
class AggregateFunctionRewriterInternal : public LogicalOperatorVisitor {
public:
	AggregateFunctionRewriterInternal(Optimizer &optimizer, AggregateRewriteRule &rule)
	    : optimizer(optimizer), rule(rule) {
	}

private:
	struct RewriteInfo {
		idx_t count_idx;
		vector<unique_ptr<Expression>> additional_expressions;
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
			AggregateFunctionRewriterInternal rewriter(optimizer, rule);
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
		const auto entry = aggregate_map.find(expr.binding);
		if (entry != aggregate_map.end()) {
			expr.binding = entry->second;
		}
		return nullptr;
	}

	void RewriteAggregates(unique_ptr<LogicalOperator> &op) {
		auto &aggr = op->Cast<LogicalAggregate>();
		if (rule.ShouldSkip(aggr)) {
			return;
		}
		const idx_t aggr_count = aggr.expressions.size();

		unordered_map<idx_t, RewriteInfo> rewrites;
		for (idx_t i = 0; i < aggr_count; ++i) {
			auto &expr = aggr.expressions[i];
			vector<reference<Expression>> bindings;
			if (!rule.matcher->Match(*expr, bindings)) {
				continue;
			}

			RewriteInfo rewrite_info;
			auto count_arg = rule.Rewrite(expr, bindings, rewrite_info.additional_expressions);

			// Add COUNT(x) to the aggregate list
			FunctionBinder function_binder(optimizer.context);
			const auto count_fun = CountFunctionBase::GetFunction();
			vector<unique_ptr<Expression>> count_args;
			count_args.push_back(std::move(count_arg));
			auto count_aggr = function_binder.BindAggregateFunction(count_fun, std::move(count_args), nullptr,
			                                                        AggregateType::NON_DISTINCT);

			rewrite_info.count_idx = aggr.expressions.size();
			rewrites.emplace(i, std::move(rewrite_info));
			aggr.expressions.push_back(std::move(count_aggr));
		}

		if (rewrites.empty()) {
			return;
		}

		// Push a projection that re-computes the original result expressions
		auto proj_index = optimizer.binder.GenerateTableIndex();
		const auto group_count = aggr.groups.size();
		vector<unique_ptr<Expression>> projection_expressions;

		// Pass group bindings through and register them in the aggregate_map
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			ColumnBinding aggregate_binding(aggr.group_index, group_idx);
			aggregate_map[aggregate_binding] = ColumnBinding(proj_index, group_idx);
			auto group_ref =
			    make_uniq<BoundColumnRefExpression>(aggr.groups[group_idx]->return_type, aggregate_binding);
			projection_expressions.push_back(std::move(group_ref));
		}

		for (idx_t i = 0; i < aggr_count; i++) {
			ColumnBinding aggregate_binding(aggr.aggregate_index, i);
			aggregate_map[aggregate_binding] = ColumnBinding(proj_index, group_count + i);
			auto &aggr_type = aggr.expressions[i]->return_type;
			auto aggr_ref = make_uniq<BoundColumnRefExpression>(aggr_type, aggregate_binding);

			const auto rewrite_entry = rewrites.find(i);
			if (rewrite_entry == rewrites.end()) {
				// Not rewritten — pass through as-is
				projection_expressions.push_back(std::move(aggr_ref));
				continue;
			}

			auto &rewrite_info = rewrite_entry->second;
			ColumnBinding count_binding(aggr.aggregate_index, rewrite_info.count_idx);
			auto count_ref = make_uniq<BoundColumnRefExpression>(aggr.expressions[rewrite_info.count_idx]->return_type,
			                                                     count_binding);

			rewrite_info.additional_expressions.push_back(std::move(count_ref));
			auto final_result = rule.CreateProjectionExpression(aggr_type, std::move(aggr_ref),
			                                                    std::move(rewrite_info.additional_expressions));
			projection_expressions.push_back(std::move(final_result));
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
	AggregateRewriteRule &rule;
	column_binding_map_t<ColumnBinding> aggregate_map;
};

AggregateFunctionRewriter::AggregateFunctionRewriter(Optimizer &optimizer) : optimizer(optimizer) {
	rules.push_back(make_uniq<AvgRewriteRule>(optimizer));
	rules.push_back(make_uniq<SumRewriteRule>(optimizer));
}

AggregateFunctionRewriter::~AggregateFunctionRewriter() {
}

void AggregateFunctionRewriter::Optimize(unique_ptr<LogicalOperator> &op) {
	// Run each rule as an independent pass so that transformations by an earlier rule affect later rules
	for (auto &rule : rules) {
		AggregateFunctionRewriterInternal rewriter(optimizer, *rule);
		rewriter.Optimize(op);
	}
}

} // namespace duckdb
