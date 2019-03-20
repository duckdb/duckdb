#include "planner/expression_binder.hpp"
#include "parser/expression/cast_expression.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(CastExpression &expr, uint32_t depth) {
	// first try to bind the child of the cast expression
	string result = Bind(&expr.child, depth);
	if (!result.empty()) {
		return BindResult(result);
	}
	// the children have been successfully resolved
	auto child = GetExpression(*expr.child);
	if (child->type == ExpressionType::VALUE_PARAMETER) {
		// parameter: move types into the parameter expression itself
		child->return_type = GetInternalType(expr.cast_type);
		child->sql_type = expr.cast_type;
	} else {
		// otherwise add a cast to the target type
		child = AddCastToType(move(child), expr.cast_type);
	}
	return BindResult(move(child));
}
