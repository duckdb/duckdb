#include "duckdb/execution/expression_executor_state.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

void ExpressionState::AddChild(Expression &child_expr) {
	types.push_back(child_expr.return_type);
	auto child_state = ExpressionExecutor::InitializeState(child_expr, root);
	child_states.push_back(std::move(child_state));

	auto expr_class = child_expr.GetExpressionClass();
	auto initialize_child = expr_class != ExpressionClass::BOUND_REF && expr_class != ExpressionClass::BOUND_CONSTANT &&
	                        expr_class != ExpressionClass::BOUND_PARAMETER;
	initialize.push_back(initialize_child);
}

void ExpressionState::Finalize() {
	if (types.empty()) {
		return;
	}
	intermediate_chunk.Initialize(GetAllocator(), types, initialize);
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
