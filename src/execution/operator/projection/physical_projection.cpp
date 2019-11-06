#include "duckdb/execution/operator/projection/physical_projection.hpp"

#include "duckdb/execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

void PhysicalProjection::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	assert(select_list.size() > 0);
	assert(children.size() == 1);
	// get the next chunk from the child
	children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
	if (state->child_chunk.size() == 0) {
		return;
	}

	ExpressionExecutor executor(state->child_chunk);
	executor.Execute(select_list, chunk);
}

string PhysicalProjection::ExtraRenderInformation() const {
	string extra_info;
	for (auto &expr : select_list) {
		extra_info += expr->GetName() + "\n";
	}
	return extra_info;
}
