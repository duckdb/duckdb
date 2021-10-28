#include "duckdb/execution/operator/scan/physical_expression_scan.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

class ExpressionScanState : public GlobalSourceState {
public:
	ExpressionScanState(const PhysicalExpressionScan &op) : expression_index(0) {
		temp_chunk.Initialize(op.GetTypes());
	}

	//! The current position in the scan
	idx_t expression_index;
	//! Temporary chunk for evaluating expressions
	DataChunk temp_chunk;
};

class ExpressionSinkState : public GlobalSinkState {
public:
	ExpressionSinkState() {
	}

	DataChunk child_chunk;
};

unique_ptr<GlobalSourceState> PhysicalExpressionScan::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<ExpressionScanState>(*this);
}

void PhysicalExpressionScan::EvaluateExpression(idx_t expression_idx, DataChunk *child_chunk, DataChunk &result) const {
	ExpressionExecutor executor(expressions[expression_idx]);
	if (child_chunk) {
		child_chunk->Verify();
		executor.Execute(*child_chunk, result);
	} else {
		executor.Execute(result);
	}
}

void PhysicalExpressionScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                     LocalSourceState &lstate) const {
	D_ASSERT(sink_state);
	auto &state = (ExpressionScanState &)gstate_p;
	auto &gstate = (ExpressionSinkState &)*sink_state;

	for (; chunk.size() < STANDARD_VECTOR_SIZE && state.expression_index < expressions.size();
	     state.expression_index++) {
		state.temp_chunk.Reset();
		EvaluateExpression(state.expression_index, &gstate.child_chunk, state.temp_chunk);
		chunk.Append(state.temp_chunk);
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

SinkResultType PhysicalExpressionScan::Sink(ExecutionContext &context, GlobalSinkState &gstate_p,
                                            LocalSinkState &lstate, DataChunk &input) const {
	auto &gstate = (ExpressionSinkState &)gstate_p;

	D_ASSERT(children.size() == 1);
	D_ASSERT(gstate.child_chunk.size() == 0);
	if (input.size() != 1) {
		throw InternalException("Expected expression scan child to have exactly one element");
	}
	gstate.child_chunk.Move(input);
	gstate.child_chunk.Verify();
	return SinkResultType::FINISHED;
}

unique_ptr<GlobalSinkState> PhysicalExpressionScan::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<ExpressionSinkState>();
}

} // namespace duckdb
