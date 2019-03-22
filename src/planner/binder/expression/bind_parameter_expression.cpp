#include "parser/expression/parameter_expression.hpp"
#include "planner/expression/bound_parameter_expression.hpp"
#include "planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(ParameterExpression &expr, uint32_t depth) {
	return BindResult(make_unique<BoundParameterExpression>(parameter_nr));
}
