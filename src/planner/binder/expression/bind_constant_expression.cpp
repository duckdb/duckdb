#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(ConstantExpression &expr, idx_t depth) {
	if (expr.value.type().id() == LogicalTypeId::VARCHAR) {
		return BindResult(make_uniq<BoundConstantExpression>(Value::STRING_LITERAL(StringValue::Get(expr.value))));
	}
	return BindResult(make_uniq<BoundConstantExpression>(expr.value));
}

} // namespace duckdb
