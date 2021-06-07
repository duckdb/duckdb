#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundCastExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.child.get());
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundCastExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	// resolve the child
	Vector child;
	child.Reference(state->intermediate_chunk.data[0]);
	auto child_state = state->child_states[0].get();

	Execute(*expr.child, child_state, sel, count, child);
	if (expr.child->return_type == expr.return_type) {
		// NOP cast
		result.Reference(child);
	} else {
		// cast it to the type specified by the cast expression
		D_ASSERT(result.GetType() == expr.return_type);
		VectorOperations::Cast(child, result, count);
	}
}

} // namespace duckdb
