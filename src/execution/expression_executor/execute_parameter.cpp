#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundParameterExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExpressionState>(expr, root);
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundParameterExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	D_ASSERT(expr.ParameterData());
	D_ASSERT(expr.ParameterData()->return_type == expr.GetReturnType());
	D_ASSERT(expr.ParameterData()->GetValue().type() == expr.GetReturnType());
	result.Reference(expr.ParameterData()->GetValue(), count_t(count));
}

} // namespace duckdb
