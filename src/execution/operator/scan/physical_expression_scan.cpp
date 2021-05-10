#include "duckdb/execution/operator/scan/physical_expression_scan.hpp"

#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

class PhysicalExpressionScanState : public PhysicalOperatorState {
public:
	PhysicalExpressionScanState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), expression_index(0) {
	}

	//! Thread safety
	mutex lock;

	//! The current position in the scan
	idx_t expression_index;

	unique_ptr<ExpressionExecutor> executor;
};

void PhysicalExpressionScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                              PhysicalOperatorState *state_p) const {
	auto state = (PhysicalExpressionScanState *)state_p;
	if (state->expression_index >= expressions.size()) {
		// finished executing all expression lists
		return;
	}

	{
		lock_guard<mutex> slock(state->lock);
		if (state->expression_index == 0) {
			// first run, fetch the chunk from the child
			D_ASSERT(children.size() == 1);
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				return;
			}
		}
	}
	// now execute the expressions of the nth expression list for the child chunk list
	// These are scanned in parallel, so the mutability is not an issue?
	state->executor = make_unique<ExpressionExecutor>(expressions[state->expression_index]);
	state->executor->Execute(state->child_chunk, chunk);

	state->expression_index++;
}

unique_ptr<PhysicalOperatorState> PhysicalExpressionScan::GetOperatorState() {
	return make_unique<PhysicalExpressionScanState>(*this, children[0].get());
}

} // namespace duckdb
