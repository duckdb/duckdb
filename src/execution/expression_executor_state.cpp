#include "duckdb/execution/expression_executor_state.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

bool ExpressionState::AddChild(Expression *child_expr) {
	types.push_back(child_expr->return_type);
	auto child_state = ExpressionExecutor::InitializeState(*child_expr, root);
	child_states.push_back(std::move(child_state));
	return child_states.back()->skip_init;
}

void ExpressionState::Finalize(const bool skip) {
	if (types.empty()) {
		return;
	}
	if (skip) {
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

ExpressionState::ExpressionState(const Expression &expr, ExpressionExecutorState &root, const bool skip_init)
    : expr(expr), root(root), skip_init(skip_init) {
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
