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
	auto &input = BoundExpression::GetExpression(*expr.input);
	auto &lower = BoundExpression::GetExpression(*expr.lower);
	auto &upper = BoundExpression::GetExpression(*expr.upper);

	auto input_sql_type = input->return_type;
	auto lower_sql_type = lower->return_type;
	auto upper_sql_type = upper->return_type;

	// cast the input types to the same type
	// now obtain the result type of the input types
	auto input_type = BoundComparisonExpression::BindComparison(input_sql_type, lower_sql_type);
	input_type = BoundComparisonExpression::BindComparison(input_type, upper_sql_type);
	// add casts (if necessary)
	input = BoundCastExpression::AddCastToType(context, std::move(input), input_type);
	lower = BoundCastExpression::AddCastToType(context, std::move(lower), input_type);
	upper = BoundCastExpression::AddCastToType(context, std::move(upper), input_type);
	if (input_type.id() == LogicalTypeId::VARCHAR) {
		// handle collation
		auto collation = StringType::GetCollation(input_type);
		input = PushCollation(context, std::move(input), collation, false);
		lower = PushCollation(context, std::move(lower), collation, false);
		upper = PushCollation(context, std::move(upper), collation, false);
	}
	if (!input->HasSideEffects() && !input->HasParameter() && !input->HasSubquery()) {
		// the expression does not have side effects and can be copied: create two comparisons
		// the reason we do this is that individual comparisons are easier to handle in optimizers
		// if both comparisons remain they will be folded together again into a single BETWEEN in the optimizer
		auto left_compare = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
		                                                         input->Copy(), std::move(lower));
		auto right_compare = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO,
		                                                          std::move(input), std::move(upper));
		return BindResult(make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
		                                                        std::move(left_compare), std::move(right_compare)));
	} else {
		// expression has side effects: we cannot duplicate it
		// create a bound_between directly
		return BindResult(
		    make_uniq<BoundBetweenExpression>(std::move(input), std::move(lower), std::move(upper), true, true));
	}
}

} // namespace duckdb
