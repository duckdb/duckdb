#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_parameter_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(BoundParameterExpression &expr, Vector &result) {
	assert(expr.value);
	assert(expr.value->type == expr.return_type);
	result.Reference(*expr.value);
}
