
#include "duckdb/optimizer/rule/timestamp_comparison.hpp"
#include "duckdb/common/constants.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/optimizer/matcher/type_matcher_id.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

TimeStampComparison::TimeStampComparison(ClientContext &context, ExpressionRewriter &rewriter)
    : Rule(rewriter), context(context) {
	// match on a ComparisonExpression that is an Equality and has a VARCHAR and ENUM as its children
	auto op = make_uniq<ComparisonExpressionMatcher>();
	op->policy = SetMatcher::Policy::UNORDERED;
	// Enum requires expression to be root
	op->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::COMPARE_EQUAL);

	// one side is timestamp cast to date
	auto left = make_uniq<CastExpressionMatcher>();
	left->type = make_uniq<TypeMatcherId>(LogicalTypeId::DATE);
	left->matcher = make_uniq<ExpressionMatcher>();
	left->matcher->type = make_uniq<TypeMatcherId>(LogicalTypeId::TIMESTAMP);
	op->matchers.push_back(std::move(left));

	// other side is varchar to date?
	auto right = make_uniq<ExpressionMatcher>();
	right->type = make_uniq<TypeMatcherId>(LogicalTypeId::DATE);
	op->matchers.push_back(std::move(right));

	root = std::move(op);
}

unique_ptr<Expression> TimeStampComparison::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                  bool &changes_made, bool is_root) {
	auto len = bindings.size();
	auto &cast_constant = bindings[len - 1].get();
	auto &cast_columnref = bindings[len - 2].get();
	auto new_expr = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);

	Value result;
	if (ExpressionExecutor::TryEvaluateScalar(context, cast_constant, result)) {
		D_ASSERT(result.type() == LogicalType::DATE);
		auto original_val = result.GetValue<duckdb::date_t>();
		auto no_seconds = dtime_t(0);

		// original date with no timestamp info
		auto original_val_ts = Value::TIMESTAMP(original_val, no_seconds);
		auto original_val_for_comparison = make_uniq<BoundConstantExpression>(original_val_ts);

		// add one day
		auto date_t_copy = result.GetValue<duckdb::date_t>();
		date_t_copy.days += 1;
		auto original_val_plus_on_date_ts = Value::TIMESTAMP(date_t_copy, dtime_t(0));

		auto val_for_comparison = make_uniq<BoundConstantExpression>(original_val_plus_on_date_ts);

		auto left_copy = cast_columnref.Copy();
		auto right_copy = cast_columnref.Copy();
		auto lt_eq_expr = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, std::move(right_copy),
		                                                       std::move(val_for_comparison));
		auto gt_eq_expr = make_uniq<BoundComparisonExpression>(
		    ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(left_copy), std::move(original_val_for_comparison));
		new_expr->children.push_back(std::move(gt_eq_expr));
		new_expr->children.push_back(std::move(lt_eq_expr));
		return std::move(new_expr);
	}

	// ok so here now I need to figure out the other stuff.
	return nullptr;
}

} // namespace duckdb
