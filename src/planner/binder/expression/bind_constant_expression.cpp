#include "parser/expression/constant_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(ConstantExpression &expr, index_t depth) {
	return BindResult(make_unique<BoundConstantExpression>(expr.value), expr.sql_type);
}
