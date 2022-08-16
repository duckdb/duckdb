#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(BetweenExpression &expr, idx_t depth) {
	// first try to bind the children of the case expression
	string error;
	BindChild(expr.input, depth, error);
	BindChild(expr.lower, depth, error);
	BindChild(expr.upper, depth, error);
	if (!error.empty()) {
		return BindResult(error);
	}
	// the children have been successfully resolved
	auto &input = (BoundExpression &)*expr.input;
	auto &lower = (BoundExpression &)*expr.lower;
	auto &upper = (BoundExpression &)*expr.upper;

	auto input_sql_type = input.expr->return_type;
	auto lower_sql_type = lower.expr->return_type;
	auto upper_sql_type = upper.expr->return_type;

	// cast the input types to the same type
	// now obtain the result type of the input types
	auto input_type = BoundComparisonExpression::BindComparison(input_sql_type, lower_sql_type);
	input_type = BoundComparisonExpression::BindComparison(input_type, upper_sql_type);
	// add casts (if necessary)
	input.expr = BoundCastExpression::AddCastToType(move(input.expr), input_type);
	lower.expr = BoundCastExpression::AddCastToType(move(lower.expr), input_type);
	upper.expr = BoundCastExpression::AddCastToType(move(upper.expr), input_type);
	if (input_type.id() == LogicalTypeId::VARCHAR) {
		// handle collation
		auto collation = StringType::GetCollation(input_type);
		input.expr = PushCollation(context, move(input.expr), collation, false);
		lower.expr = PushCollation(context, move(lower.expr), collation, false);
		upper.expr = PushCollation(context, move(upper.expr), collation, false);
	}
	if (!input.expr->HasSideEffects() && !input.expr->HasParameter() && !input.expr->HasSubquery()) {
		// the expression does not have side effects and can be copied: create two comparisons
		// the reason we do this is that individual comparisons are easier to handle in optimizers
		// if both comparisons remain they will be folded together again into a single BETWEEN in the optimizer
		auto left_compare = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
		                                                           input.expr->Copy(), move(lower.expr));
		auto right_compare = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO,
		                                                            move(input.expr), move(upper.expr));
		return BindResult(make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(left_compare),
		                                                          move(right_compare)));
	} else {
		// expression has side effects: we cannot duplicate it
		// create a bound_between directly
		return BindResult(
		    make_unique<BoundBetweenExpression>(move(input.expr), move(lower.expr), move(upper.expr), true, true));
	}
}

} // namespace duckdb
