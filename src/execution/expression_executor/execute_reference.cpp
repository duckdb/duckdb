#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundReferenceExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundReferenceExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	D_ASSERT(expr.index != DConstants::INVALID_INDEX);
	D_ASSERT(expr.index < chunk->ColumnCount());
#ifdef DEBUG
	// Input Vector and result Vector should not be the same
	auto input_aux = chunk->data[expr.index].GetAuxiliary();
	auto input_buffer = chunk->data[expr.index].GetBuffer();
	auto result_aux = result.GetAuxiliary();
	auto result_buffer = result.GetBuffer();
	D_ASSERT(input_aux != result_aux || input_aux == nullptr);
	D_ASSERT(input_buffer != result_buffer || input_buffer == nullptr);
#endif
	if (sel) {
		result.Slice(chunk->data[expr.index], *sel, count);
	} else {
		result.Reference(chunk->data[expr.index]);
	}
}

} // namespace duckdb
