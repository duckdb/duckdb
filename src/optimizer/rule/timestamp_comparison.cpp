
#include "duckdb/optimizer/rule/timestamp_comparison.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/optimizer/matcher/type_matcher_id.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/operator/add.hpp"

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
	left->matcher->expr_class = ExpressionClass::BOUND_COLUMN_REF;
	left->matcher->type = make_uniq<TypeMatcherId>(LogicalTypeId::TIMESTAMP);
	op->matchers.push_back(std::move(left));

	// other side is varchar to date?
	auto right = make_uniq<CastExpressionMatcher>();
	right->type = make_uniq<TypeMatcherId>(LogicalTypeId::DATE);
	right->matcher = make_uniq<FoldableConstantMatcher>();
	right->matcher->expr_class = ExpressionClass::BOUND_CONSTANT;
	right->matcher->type = make_uniq<TypeMatcherId>(LogicalTypeId::VARCHAR);
	op->matchers.push_back(std::move(right));

	root = std::move(op);
}

static void ExpressionIsConstant(Expression &expr, bool &is_constant) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		is_constant = false;
		return;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { ExpressionIsConstant(child, is_constant); });
}

unique_ptr<Expression> TimeStampComparison::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                  bool &changes_made, bool is_root) {
	auto cast_constant = bindings[3].get().Copy();
	auto cast_columnref = bindings[2].get().Copy();
	auto is_constant = true;
	ExpressionIsConstant(*cast_constant, is_constant);
	if (!is_constant) {
		// means the matchers are flipped, so we need to flip our bindings
		// for some reason an extra binding is added in this case.
		cast_constant = bindings[4].get().Copy();
		cast_columnref = bindings[3].get().Copy();
	}
	auto new_expr = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);

	Value result;
	if (ExpressionExecutor::TryEvaluateScalar(context, *cast_constant, result)) {
		D_ASSERT(result.type() == LogicalType::DATE);
		auto original_val = result.GetValue<duckdb::date_t>();
		auto no_seconds = dtime_t(0);

		// original date as timestamp with no seconds
		auto original_val_ts = Value::TIMESTAMP(original_val, no_seconds);
		auto original_val_for_comparison = make_uniq<BoundConstantExpression>(original_val_ts);

		// add one day and validate the new date
		// code is inspired by AddOperator::Operation(date_t left, int32_t right). The function wasn't used directly
		// since it throws errors that I cannot catch here.

		auto date_t_copy = result.GetValue<duckdb::date_t>();
		date_t date_t_result;
		// attempt to add 1 day
		if (!TryAddOperator::Operation<date_t, int32_t, date_t>(date_t_copy, 1, date_t_result)) {
			// don't rewrite the expression and let the expression executor handle the invalid date
			return nullptr;
		}

		auto result_as_val = Value::DATE(date_t_result);
		auto original_val_plus_on_date_ts = Value::TIMESTAMP(result_as_val.GetValue<timestamp_t>());

		auto val_for_comparison = make_uniq<BoundConstantExpression>(original_val_plus_on_date_ts);

		auto left_copy = cast_columnref->Copy();
		auto right_copy = cast_columnref->Copy();
		auto lt_eq_expr = make_uniq_base<Expression, BoundComparisonExpression>(
		    ExpressionType::COMPARE_LESSTHAN, std::move(right_copy), std::move(val_for_comparison));
		auto gt_eq_expr = make_uniq_base<Expression, BoundComparisonExpression>(
		    ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(left_copy), std::move(original_val_for_comparison));
		new_expr->children.push_back(std::move(gt_eq_expr));
		new_expr->children.push_back(std::move(lt_eq_expr));
		return std::move(new_expr);
	}
	return nullptr;
}

} // namespace duckdb
