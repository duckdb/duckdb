#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundCastExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.child.get());
	return result;
}

void ExpressionExecutor::Execute(BoundCastExpression &expr, ExpressionState *state, Vector &result) {
	// resolve the child
	Vector child(GetCardinality(), expr.child->return_type);
	auto child_state = state->child_states[0].get();

	Execute(*expr.child, child_state, child);
	if (child.type == expr.return_type) {
		// NOP cast
		result.Reference(child);
	} else {
		// cast it to the type specified by the cast expression
		VectorOperations::Cast(child, result, expr.source_type, expr.target_type);
	}
}
