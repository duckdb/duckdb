#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundParameterExpression &expr,
                                                                ExpressionExecutorState &root) {
	return nullptr;
}

void ExpressionExecutor::Execute(BoundParameterExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	D_ASSERT(expr.value);
	D_ASSERT(expr.value->type() == expr.return_type);
	result.Reference(*expr.value);
}

} // namespace duckdb
