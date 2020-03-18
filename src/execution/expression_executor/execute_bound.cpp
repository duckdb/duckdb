#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundReferenceExpression &expr,
                                                                ExpressionExecutorState &root) {
	return nullptr;
}

void ExpressionExecutor::Execute(BoundReferenceExpression &expr, ExpressionState *state, Vector &result, idx_t count) {
	assert(expr.index != INVALID_INDEX);
	assert(expr.index < chunk->column_count());
	result.Reference(chunk->data[expr.index]);
}
