
#include "execution/operator/projection/physical_projection.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

void PhysicalProjection::_GetChunk(ClientContext &context, DataChunk &chunk,
                                   PhysicalOperatorState *state) {
	chunk.Reset();
	assert(select_list.size() > 0);
	assert(children.size() == 1);
	// get the next chunk from the child
	children[0]->GetChunk(context, state->child_chunk,
	                      state->child_state.get());
	if (state->child_chunk.size() == 0) {
		return;
	}

	ExpressionExecutor executor(state, context);
	executor.Execute(select_list, chunk);
}

unique_ptr<PhysicalOperatorState>
PhysicalProjection::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalOperatorState>(
	    children.size() == 0 ? nullptr : children[0].get(), parent_executor);
}
