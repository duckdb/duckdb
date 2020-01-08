#include "duckdb/execution/operator/helper/physical_execute.hpp"

using namespace duckdb;
using namespace std;

void PhysicalExecute::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	assert(plan);
	plan->GetChunk(context, chunk, state_);
}

unique_ptr<PhysicalOperatorState> PhysicalExecute::GetOperatorState() {
	return plan->GetOperatorState();
}
