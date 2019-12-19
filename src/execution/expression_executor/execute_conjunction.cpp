#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundConjunctionExpression &expr, ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddIntermediates({expr.left.get(), expr.right.get()});
	return result;
}

void ExpressionExecutor::Execute(BoundConjunctionExpression &expr, ExpressionState *state, Vector &result) {
	// resolve the children
	auto &left = state->arguments.data[0];
	auto &right = state->arguments.data[1];
	Execute(*expr.left, state->child_states[0].get(), left);
	Execute(*expr.right, state->child_states[1].get(), right);

	switch (expr.type) {
	case ExpressionType::CONJUNCTION_AND:
		VectorOperations::And(left, right, result);
		break;
	case ExpressionType::CONJUNCTION_OR:
		VectorOperations::Or(left, right, result);
		break;
	default:
		throw NotImplementedException("Unknown conjunction type!");
	}
}
