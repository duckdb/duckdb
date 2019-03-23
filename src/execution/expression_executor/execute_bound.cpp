#include "execution/expression_executor.hpp"
#include "planner/expression/bound_reference_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(BoundReferenceExpression &expr, Vector &result) {
	assert(expr.index != (uint32_t)-1);
	assert(expr.index < chunk->column_count);
	result.Reference(chunk->data[expr.index]);
}
