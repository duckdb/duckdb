#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundCastExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExecuteFunctionState>(expr, root);
	result->AddChild(*expr.child);
	result->Finalize();

	if (expr.bound_cast.init_local_state) {
		auto context_ptr = root.executor->HasContext() ? &root.executor->GetContext() : nullptr;
		CastLocalStateParameters parameters(context_ptr, expr.bound_cast.cast_data);
		result->local_state = expr.bound_cast.init_local_state(parameters);
	}
	return std::move(result);
}

void ExpressionExecutor::Execute(const BoundCastExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	auto lstate = ExecuteFunctionState::GetFunctionState(*state);

	// resolve the child
	state->intermediate_chunk.Reset();

	auto &child = state->intermediate_chunk.data[0];
	auto child_state = state->child_states[0].get();

	Execute(*expr.child, child_state, sel, count, child);

	string error_message;
	auto error_ref = expr.try_cast ? &error_message : nullptr;
	CastParameters parameters(expr.bound_cast.cast_data.get(), false, error_ref, lstate);
	parameters.query_location = expr.GetQueryLocation();
	parameters.cast_source = expr.child.get();
	parameters.cast_target = expr;
	expr.bound_cast.function(child, result, count, parameters);
}

} // namespace duckdb
