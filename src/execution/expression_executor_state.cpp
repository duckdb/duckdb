#include "duckdb/execution/expression_executor_state.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

void ExpressionState::AddChild(Expression *expr) {
	types.push_back(expr->return_type);
	child_states.push_back(ExpressionExecutor::InitializeState(*expr, root));
}

void ExpressionState::Finalize(bool empty) {
	if (types.empty()) {
		return;
	}
	if (empty) {
		intermediate_chunk.InitializeEmpty(types);
	} else {
		intermediate_chunk.Initialize(GetAllocator(), types);
	}
}

Allocator &ExpressionState::GetAllocator() {
	return root.executor->GetAllocator();
}

bool ExpressionState::HasContext() {
	return root.executor->HasContext();
}

ClientContext &ExpressionState::GetContext() {
	if (!HasContext()) {
		throw BinderException("Cannot use %s in this context", (expr.Cast<BoundFunctionExpression>()).function.name);
	}
	return root.executor->GetContext();
}

ExpressionState::ExpressionState(const Expression &expr, ExpressionExecutorState &root) : expr(expr), root(root) {
}

ExpressionExecutorState::ExpressionExecutorState() {
}

void ExpressionState::Verify(ExpressionExecutorState &root_executor) {
	D_ASSERT(&root_executor == &root);
	for (auto &entry : child_states) {
		entry->Verify(root_executor);
	}
}

void ExpressionExecutorState::Verify() {
	D_ASSERT(executor);
	root_state->Verify(*this);
}

} // namespace duckdb
