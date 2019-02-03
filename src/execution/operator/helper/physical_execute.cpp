#include "execution/operator/helper/physical_execute.hpp"

using namespace duckdb;
using namespace std;

void PhysicalExecute::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	assert(plan);
	plan->GetChunk(context, chunk, state_);
}
