#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundFunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	vector<Expression *> children;
	for (auto &child : expr.children) {
		children.push_back(child.get());
	}
	result->AddIntermediates(children);
	return result;
}

void ExpressionExecutor::Execute(BoundFunctionExpression &expr, ExpressionState *state, Vector &result) {
	for (index_t i = 0; i < expr.children.size(); i++) {
		Execute(*expr.children[i], state->child_states[i].get(), state->arguments.data[i]);
	}
	expr.function.function(state->arguments, *state, result);
	if (result.type != expr.return_type) {
		throw TypeMismatchException(expr.return_type, result.type,
		                            "expected function to return the former "
		                            "but the function returned the latter");
	}
}
