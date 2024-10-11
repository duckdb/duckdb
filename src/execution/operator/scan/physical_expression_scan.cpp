#include "duckdb/execution/operator/scan/physical_expression_scan.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

class ExpressionScanState : public OperatorState {
public:
	explicit ExpressionScanState(Allocator &allocator, const PhysicalExpressionScan &op) : expression_index(0) {
		temp_chunk.Initialize(allocator, op.GetTypes());
	}

	//! The current position in the scan
	idx_t expression_index;
	//! Temporary chunk for evaluating expressions
	DataChunk temp_chunk;
};

unique_ptr<OperatorState> PhysicalExpressionScan::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<ExpressionScanState>(Allocator::Get(context.client), *this);
}

OperatorResultType PhysicalExpressionScan::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<ExpressionScanState>();

	for (; chunk.size() + input.size() <= STANDARD_VECTOR_SIZE && state.expression_index < expressions.size();
	     state.expression_index++) {
		state.temp_chunk.Reset();
		EvaluateExpression(context.client, state.expression_index, &input, chunk, &state.temp_chunk);
	}
	if (state.expression_index < expressions.size()) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	} else {
		state.expression_index = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}
}

void PhysicalExpressionScan::EvaluateExpression(ClientContext &context, idx_t expression_idx,
                                                optional_ptr<DataChunk> child_chunk, DataChunk &result,
                                                optional_ptr<DataChunk> temp_chunk_ptr) const {
	if (temp_chunk_ptr) {
		EvaluateExpressionInternal(context, expression_idx, child_chunk, result, *temp_chunk_ptr);
	} else {
		DataChunk temp_chunk;
		temp_chunk.Initialize(Allocator::Get(context), GetTypes());
		EvaluateExpressionInternal(context, expression_idx, child_chunk, result, temp_chunk);
	}
}

void PhysicalExpressionScan::EvaluateExpressionInternal(ClientContext &context, idx_t expression_idx,
                                                        optional_ptr<DataChunk> child_chunk, DataChunk &result,
                                                        DataChunk &temp_chunk) const {
	ExpressionExecutor executor(context, expressions[expression_idx]);
	if (child_chunk) {
		child_chunk->Verify();
		executor.Execute(*child_chunk, temp_chunk);
	} else {
		executor.Execute(temp_chunk);
	}
	// Need to append because "executor" might be holding state (e.g., strings), which go out of scope here
	result.Append(temp_chunk);
}

bool PhysicalExpressionScan::IsFoldable() const {
	for (auto &expr_list : expressions) {
		for (auto &expr : expr_list) {
			if (!expr->IsFoldable()) {
				return false;
			}
		}
	}
	return true;
}

} // namespace duckdb
