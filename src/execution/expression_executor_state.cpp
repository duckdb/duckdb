#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

void ExpressionState::AddChild(Expression *expr) {
	types.push_back(expr->return_type);
	child_states.push_back(ExpressionExecutor::InitializeState(*expr, root));
}

void ExpressionState::Finalize() {
	if (!types.empty()) {
		intermediate_chunk.Initialize(GetAllocator(), types);
	}
}

Allocator &ExpressionState::GetAllocator() {
	return root.executor->GetAllocator();
}

ClientContext &ExpressionState::GetContext() {
	return root.executor->GetContext();
}

ExpressionState::ExpressionState(const Expression &expr, ExpressionExecutorState &root)
    : expr(expr), root(root), name(expr.ToString()) {
}

ExpressionExecutorState::ExpressionExecutorState(const string &name) : profiler(), name(name) {
}
} // namespace duckdb
