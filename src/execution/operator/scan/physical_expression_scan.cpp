#include "duckdb/execution/operator/scan/physical_expression_scan.hpp"

#include "duckdb/execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

class PhysicalExpressionScanState : public PhysicalOperatorState {
public:
	PhysicalExpressionScanState(PhysicalOperator *child) : PhysicalOperatorState(child), expression_index(0) {
	}

	//! The current position in the scan
	idx_t expression_index;

	unique_ptr<ExpressionExecutor> executor;
};

void PhysicalExpressionScan::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = (PhysicalExpressionScanState *)state_;
	if (state->expression_index >= expressions.size()) {
		// finished executing all expression lists
		return;
	}

	if (state->expression_index == 0) {
		// first run, fetch the chunk from the child
		assert(children.size() == 1);
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
	}
	// now execute the expressions of the nth expression list for the child chunk list
	state->executor = make_unique<ExpressionExecutor>(expressions[state->expression_index]);
	state->executor->Execute(state->child_chunk, chunk);

	state->expression_index++;
}

unique_ptr<PhysicalOperatorState> PhysicalExpressionScan::GetOperatorState() {
	return make_unique<PhysicalExpressionScanState>(children[0].get());
}
