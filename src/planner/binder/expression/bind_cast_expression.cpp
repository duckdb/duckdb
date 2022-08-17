#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(CastExpression &expr, idx_t depth) {
	// first try to bind the child of the cast expression
	string error = Bind(&expr.child, depth);
	if (!error.empty()) {
		return BindResult(error);
	}
	// FIXME: We can also implement 'hello'::schema.custom_type; and pass by the schema down here.
	// Right now just considering its DEFAULT_SCHEMA always
	Binder::BindLogicalType(context, expr.cast_type, DEFAULT_SCHEMA);
	// the children have been successfully resolved
	auto &child = (BoundExpression &)*expr.child;
	if (expr.try_cast) {
		if (child.expr->return_type == expr.cast_type) {
			// no cast required: type matches
			return BindResult(move(child.expr));
		}
		child.expr = make_unique<BoundCastExpression>(move(child.expr), expr.cast_type, true);
	} else {
		// otherwise add a cast to the target type
		child.expr = BoundCastExpression::AddCastToType(move(child.expr), expr.cast_type);
	}
	return BindResult(move(child.expr));
}
} // namespace duckdb
