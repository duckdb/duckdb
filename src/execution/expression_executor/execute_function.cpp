#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

ExecuteFunctionState::ExecuteFunctionState(const Expression &expr, ExpressionExecutorState &root)
    : ExpressionState(expr, root) {
}

ExecuteFunctionState::~ExecuteFunctionState() {
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundFunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExecuteFunctionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	result->Finalize();
	if (expr.function.init_local_state) {
		result->local_state = expr.function.init_local_state(expr, expr.bind_info.get());
	}
	return move(result);
}

void ExpressionExecutor::Execute(const BoundFunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	state->intermediate_chunk.Reset();
	auto &arguments = state->intermediate_chunk;
	if (!state->types.empty()) {
		for (idx_t i = 0; i < expr.children.size(); i++) {
			D_ASSERT(state->types[i] == expr.children[i]->return_type);
			Execute(*expr.children[i], state->child_states[i].get(), sel, count, arguments.data[i]);
#ifdef DEBUG
			if (expr.children[i]->return_type.id() == LogicalTypeId::VARCHAR) {
				arguments.data[i].UTFVerify(count);
			}
#endif
		}
		arguments.Verify();
	}
	arguments.SetCardinality(count);
	state->profiler.BeginSample();
	expr.function.function(arguments, *state, result);
	state->profiler.EndSample(count);
	D_ASSERT(result.GetType() == expr.return_type);
}

} // namespace duckdb
