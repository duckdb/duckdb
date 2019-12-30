#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(BetweenExpression &expr, index_t depth) {
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
	// cast the input types to the same type
	// now obtain the result type of the input types
	auto input_type = MaxSQLType(MaxSQLType(input.sql_type, lower.sql_type), upper.sql_type);
	if (input_type.id == SQLTypeId::VARCHAR) {
		// for comparison with strings, we prefer to bind to the numeric types
		if (input.sql_type.IsNumeric()) {
			input_type = input.sql_type;
		}
		if (lower.sql_type.IsNumeric()) {
			input_type = lower.sql_type;
		}
		if (upper.sql_type.IsNumeric()) {
			input_type = upper.sql_type;
		}
	}
	if (input_type.id == SQLTypeId::UNKNOWN) {
		throw BinderException("Could not determine type of parameters: try adding explicit type casts");
	}
	// add casts (if necessary)
	input.expr = BoundCastExpression::AddCastToType(move(input.expr), input.sql_type, input_type);
	lower.expr = BoundCastExpression::AddCastToType(move(lower.expr), lower.sql_type, input_type);
	upper.expr = BoundCastExpression::AddCastToType(move(upper.expr), upper.sql_type, input_type);
	// now create the bound comparison expression
	return BindResult(make_unique<BoundBetweenExpression>(move(input.expr), move(lower.expr), move(upper.expr), expr.lower_inclusive, expr.upper_inclusive),
	                  SQLType(SQLTypeId::BOOLEAN));
}
