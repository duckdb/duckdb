#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundReferenceExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExpressionState>(expr, root);
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundReferenceExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	D_ASSERT(expr.Index() != DConstants::INVALID_INDEX);
	D_ASSERT(expr.Index() < chunk->ColumnCount());

	if (sel) {
		result.Slice(chunk->data[expr.Index()], *sel, count);
	} else {
		result.Reference(chunk->data[expr.Index()]);
	}
}

} // namespace duckdb
