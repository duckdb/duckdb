#include "duckdb/execution/operator/scan/physical_expression_scan.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"

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
	return make_unique<ExpressionScanState>(Allocator::Get(context.client), *this);
}

OperatorResultType PhysicalExpressionScan::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (ExpressionScanState &)state_p;

	for (; chunk.size() + input.size() <= STANDARD_VECTOR_SIZE && state.expression_index < expressions.size();
	     state.expression_index++) {
		state.temp_chunk.Reset();
		EvaluateExpression(Allocator::Get(context.client), state.expression_index, &input, state.temp_chunk);
		chunk.Append(state.temp_chunk);
	}
	if (state.expression_index < expressions.size()) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	} else {
		state.expression_index = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}
}

void PhysicalExpressionScan::EvaluateExpression(Allocator &allocator, idx_t expression_idx, DataChunk *child_chunk,
                                                DataChunk &result) const {
	ExpressionExecutor executor(allocator, expressions[expression_idx]);
	if (child_chunk) {
		child_chunk->Verify();
		executor.Execute(*child_chunk, result);
	} else {
		executor.Execute(result);
	}
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
