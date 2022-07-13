#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(CollateExpression &expr, idx_t depth) {
	// first try to bind the child of the cast expression
	string error = Bind(&expr.child, depth);
	if (!error.empty()) {
		return BindResult(error);
	}
	auto &child = (BoundExpression &)*expr.child;
	if (child.expr->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (child.expr->return_type.id() != LogicalTypeId::VARCHAR) {
		throw BinderException("collations are only supported for type varchar");
	}
	child.expr->return_type = LogicalType::VARCHAR_COLLATION(expr.collation);
	return BindResult(move(child.expr));
}

} // namespace duckdb
