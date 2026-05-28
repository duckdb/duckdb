#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(CastExpression &expr, idx_t depth) {
	// first try to bind the child of the cast expression
	auto error = Bind(expr.ChildMutable(), depth);
	if (error.HasError()) {
		return BindResult(std::move(error));
	}
	// FIXME: We can also implement 'hello'::schema.custom_type; and pass by the schema down here.
	// Right now just considering its DEFAULT_SCHEMA always
	binder.BindLogicalType(expr.TargetTypeMutable());
	// the children have been successfully resolved
	auto &child = BoundExpression::GetExpression(*expr.ChildMutable());
	if (expr.IsTryCast()) {
		if (ExpressionBinder::GetExpressionReturnType(*child) == expr.TargetType()) {
			// no cast required: type matches
			return BindResult(std::move(child));
		}
		child = BoundCastExpression::AddCastToType(context, std::move(child), expr.TargetType(), true);
	} else {
		// otherwise add a cast to the target type
		child = BoundCastExpression::AddCastToType(context, std::move(child), expr.TargetType());
	}
	return BindResult(std::move(child));
}
} // namespace duckdb
