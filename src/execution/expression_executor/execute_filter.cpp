#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

namespace duckdb {


unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundFilterExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.filter.get());
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(BoundFilterExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t &count, Vector &result) {

//	result.Reference(chunk->data[0]);
//
//	SelectionVector true_sel(STANDARD_VECTOR_SIZE), false_sel(STANDARD_VECTOR_SIZE);
//	count = Select(*expr.filter, state->child_states[0].get(), sel, count, &true_sel, &false_sel);
//	result.Slice(true_sel,count);
//	expr.return_type = result.type;

}



} // namespace duckdb
