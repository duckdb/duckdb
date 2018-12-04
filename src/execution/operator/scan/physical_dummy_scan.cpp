
#include "execution/operator/scan/physical_dummy_scan.hpp"

using namespace duckdb;
using namespace std;

void PhysicalDummyScan::_GetChunk(ClientContext &context, DataChunk &chunk,
                                  PhysicalOperatorState *state) {
	chunk.Reset();
	if (state->finished) {
		return;
	}
	state->finished = true;
	if (this->chunk.column_count > 0) {
		this->chunk.Move(chunk);
	} else {
		chunk.data[0].count = 1;
	}
}

unique_ptr<PhysicalOperatorState>
PhysicalDummyScan::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalOperatorState>(nullptr, parent_executor);
}
