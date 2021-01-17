#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundColumnRefExpression &expr,
                                                                ExpressionExecutorState &root) {
	return nullptr;
}

void ExpressionExecutor::Execute(BoundColumnRefExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
    D_ASSERT(expr.binding.column_index != INVALID_INDEX);
	D_ASSERT(expr.binding.column_index< chunk->ColumnCount());
	if (sel) {
		result.Slice(chunk->data[ expr.binding.column_index], *sel, count);
	} else {
		result.Reference(chunk->data[  expr.binding.column_index]);
	}
}

} // namespace duckdb
