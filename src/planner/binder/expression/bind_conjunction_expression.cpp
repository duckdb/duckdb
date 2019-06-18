#include "parser/expression/conjunction_expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(ConjunctionExpression &expr, index_t depth) {
	// first try to bind the children of the case expression
	string error;
	BindChild(expr.left, depth, error);
	BindChild(expr.right, depth, error);
	if (!error.empty()) {
		return BindResult(error);
	}
	// the children have been successfully resolved
	auto &left = (BoundExpression &)*expr.left;
	auto &right = (BoundExpression &)*expr.right;
	// cast the input types to boolean (if necessary)
	left.expr = AddCastToType(move(left.expr), left.sql_type, SQLType(SQLTypeId::BOOLEAN));
	right.expr = AddCastToType(move(right.expr), right.sql_type, SQLType(SQLTypeId::BOOLEAN));
	// now create the bound conjunction expression
	return BindResult(make_unique<BoundConjunctionExpression>(expr.type, move(left.expr), move(right.expr)),
	                  SQLType(SQLTypeId::BOOLEAN));
}
