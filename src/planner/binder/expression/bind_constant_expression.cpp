#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {
using namespace std;

BindResult ExpressionBinder::BindExpression(ConstantExpression &expr, idx_t depth) {
	expr.value.SetLogicalType(expr.sql_type);
	return BindResult(make_unique<BoundConstantExpression>(expr.value));
}

} // namespace duckdb
