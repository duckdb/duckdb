#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(BoundReferenceExpression &expr, Vector &result) {
	assert(expr.index != INVALID_INDEX);
	assert(expr.index < chunk->column_count);
	result.Reference(chunk->data[expr.index]);
}
