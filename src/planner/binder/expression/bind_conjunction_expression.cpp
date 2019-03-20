#include "planner/expression_binder.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "parser/expression/conjunction_expression.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(ConjunctionExpression &expr, uint32_t depth) {
	// first try to bind the children of the case expression
	string left_result = Bind(&expr.left, depth);
	string right_result = Bind(&expr.right, depth);
	if (!left_result.empty()) {
		return BindResult(left_result);
	}
	if (!right_result.empty()) {
		return BindResult(right_result);
	}
	// the children have been successfully resolved
	auto left = GetExpression(*expr.left);
	auto right = GetExpression(*expr.right);
	// cast the input types to boolean (if necessary)
	left = AddCastToType(move(left), SQLType(SQLTypeId::BOOLEAN));
	right = AddCastToType(move(right), SQLType(SQLTypeId::BOOLEAN));
	// now create the bound conjunction expression
	return BindResult(make_unique<BoundConjunctionExpression>(expr.type, move(left), move(right));
}
