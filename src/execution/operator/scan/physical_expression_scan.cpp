#include "execution/operator/scan/physical_expression_scan.hpp"

#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

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
	ExpressionExecutor executor(state->child_chunk);
	executor.Execute(expressions[state->expression_index], chunk);

	state->expression_index++;
}

unique_ptr<PhysicalOperatorState> PhysicalExpressionScan::GetOperatorState() {
	return make_unique<PhysicalExpressionScanState>(children[0].get());
}
