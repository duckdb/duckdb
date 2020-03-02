#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(CastExpression &expr, idx_t depth) {
	// first try to bind the child of the cast expression
	string error = Bind(&expr.child, depth);
	if (!error.empty()) {
		return BindResult(error);
	}
	// the children have been successfully resolved
	auto &child = (BoundExpression &)*expr.child;
	if (child.expr->type == ExpressionType::VALUE_PARAMETER) {
		auto &parameter = (BoundParameterExpression &)*child.expr;
		// parameter: move types into the parameter expression itself
		parameter.return_type = GetInternalType(expr.cast_type);
		parameter.sql_type = expr.cast_type;
	} else {
		// otherwise add a cast to the target type
		child.expr = BoundCastExpression::AddCastToType(move(child.expr), child.sql_type, expr.cast_type);
	}
	return BindResult(move(child.expr), expr.cast_type);
}
