#include "execution/operator/helper/physical_execute.hpp"

using namespace duckdb;
using namespace std;

void PhysicalExecute::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	assert(plan);
	plan->GetChunk(context, chunk, state_);
}

unique_ptr<PhysicalOperatorState> PhysicalExecute::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalOperatorState>(plan->children[0].get(), parent_executor);
}
