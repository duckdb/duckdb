#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct FunctionExpressionState : public ExpressionState {
	FunctionExpressionState(const Expression &expr, ExpressionExecutorState &root) : ExpressionState(expr, root) {
	}

	DataChunk arguments;
};
unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundFunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<FunctionExpressionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	result->Finalize();
	if (!result->types.empty()) {
		result->arguments.InitializeEmpty(result->types);
	}
	return move(result);
}

void ExpressionExecutor::Execute(const BoundFunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	auto &fstate = (FunctionExpressionState &)*state;
	auto &arguments = fstate.arguments;
	if (!state->types.empty()) {
		arguments.Reference(state->intermediate_chunk);
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
	if (current_count >= next_sample) {
		state->profiler.Start();
	}
	expr.function.function(arguments, *state, result);
	if (current_count >= next_sample) {
		state->profiler.End();
		state->time += state->profiler.Elapsed();
	}
	if (result.GetType() != expr.return_type) {
		throw TypeMismatchException(expr.return_type, result.GetType(),
		                            "expected function to return the former "
		                            "but the function returned the latter");
	}
}

} // namespace duckdb
