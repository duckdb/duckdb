#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundReferenceExpression &expr,
                                                                ExpressionExecutorState &root) {
	return nullptr;
}

void ExpressionExecutor::Execute(BoundReferenceExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	D_ASSERT(expr.index != INVALID_INDEX);
	D_ASSERT(expr.index < chunk->ColumnCount());
	if (sel) {
		result.Slice(chunk->data[expr.index], *sel, count);
	} else {
		result.Reference(chunk->data[expr.index]);
	}
}

} // namespace duckdb
