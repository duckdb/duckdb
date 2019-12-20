#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundConjunctionExpression &expr, ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	vector<Expression*> children;
	for(auto &child : expr.children) {
		children.push_back(child.get());
	}
	result->AddIntermediates(children);
	return result;
}

void ExpressionExecutor::Execute(BoundConjunctionExpression &expr, ExpressionState *state, Vector &result) {
	// execute the children
	for (index_t i = 0; i < expr.children.size(); i++) {
		Execute(*expr.children[i], state->child_states[i].get(), state->arguments.data[i]);
		if (i == 0) {
			// move the result
			result.Reference(state->arguments.data[i]);
		} else {
			Vector intermediate(TypeId::BOOLEAN, true, false);
			// AND/OR together
			switch (expr.type) {
			case ExpressionType::CONJUNCTION_AND:
				VectorOperations::And(state->arguments.data[i], result, intermediate);
				break;
			case ExpressionType::CONJUNCTION_OR:
				VectorOperations::Or(state->arguments.data[i], result, intermediate);
				break;
			default:
				throw NotImplementedException("Unknown conjunction type!");
			}
			intermediate.Move(result);
		}
	}

}
