#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundParameterExpression &expr,
                                                                ExpressionExecutorState &root) {
	return nullptr;
}

void ExpressionExecutor::Execute(BoundParameterExpression &expr, ExpressionState *state, Vector &result, idx_t count) {
	assert(expr.value);
	assert(expr.value->type == expr.return_type);
	result.Reference(*expr.value);
}
