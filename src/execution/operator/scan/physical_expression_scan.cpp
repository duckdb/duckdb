#include "duckdb/execution/operator/scan/physical_expression_scan.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

class ExpressionScanState : public GlobalSourceState {
public:
	ExpressionScanState() : expression_index(0) {
	}

	//! The current position in the scan
	idx_t expression_index;
	//! Expression executor for the current set of expressions
	unique_ptr<ExpressionExecutor> executor;
};

class ExpressionSinkState : public GlobalSinkState {
public:
	ExpressionSinkState() {
	}

	DataChunk child_chunk;
};

unique_ptr<GlobalSourceState> PhysicalExpressionScan::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<ExpressionScanState>();
}

void PhysicalExpressionScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                     LocalSourceState &lstate) const {
	auto &state = (ExpressionScanState &)gstate;
	if (state.expression_index >= expressions.size()) {
		// finished executing all expression lists
		return;
	}

	// execute the expressions of the nth expression list for the child chunk list
	state.executor = make_unique<ExpressionExecutor>(expressions[state.expression_index]);
	if (sink_state) {
		auto &gstate = (ExpressionSinkState &)*sink_state;
		gstate.child_chunk.Verify();
		state.executor->Execute(gstate.child_chunk, chunk);
	} else {
		state.executor->Execute(chunk);
	}

	state.expression_index++;
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
