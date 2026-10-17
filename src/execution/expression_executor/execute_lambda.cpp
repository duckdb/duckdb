#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundLambdaExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExpressionState>(expr, root);
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundLambdaExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	// a lambda carries no value of its own - the function it is passed to executes its body separately.
	// write a constant placeholder so that the argument positions line up with the function's arguments
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::Validity(result).SetAllValid(1);
	*ConstantVector::GetData<uint8_t>(result) = 0;
}

} // namespace duckdb
