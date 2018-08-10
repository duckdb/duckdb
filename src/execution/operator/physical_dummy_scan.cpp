
#include "execution/operator/physical_dummy_scan.hpp"

using namespace duckdb;
using namespace std;

vector<TypeId> PhysicalDummyScan::GetTypes() { return {TypeId::INTEGER}; }

void PhysicalDummyScan::GetChunk(DataChunk &chunk,
                                 PhysicalOperatorState *state) {
	chunk.Reset();
	if (state->finished) {
		return;
	}
	state->finished = true;

	chunk.data[0]->count = 1;
	chunk.count = chunk.data[0]->count;
}

unique_ptr<PhysicalOperatorState>
PhysicalDummyScan::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalOperatorState>(nullptr, parent_executor);
}
