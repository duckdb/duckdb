
#include "execution/operator/physical_projection.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

void PhysicalProjection::InitializeChunk(DataChunk &chunk) {
	// just copy the chunk data of the child
	vector<TypeId> types;
	for (auto &expr : select_list) {
		types.push_back(expr->return_type);
	}
	chunk.Initialize(types);
}

void PhysicalProjection::GetChunk(DataChunk &chunk,
                                  PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalProjectionOperatorState *>(state_);
	chunk.Reset();

	if (children.size() > 0) {
		// get the next chunk from the child, if there is a child
		children[0]->GetChunk(state->child_chunk, state->child_state.get());
		if (state->child_chunk.count == 0) {
			return;
		}
	} else {
		// no FROM clause, set a simple marker to ensure the projection is only
		// executed once
		if (state->executed) {
			return;
		}
		state->executed = true;
	}

	ExpressionExecutor executor(state->child_chunk);

	size_t i = 0;
	for (size_t i = 0; i < select_list.size(); i++) {
		auto &expr = select_list[i];
		executor.Execute(expr.get(), *chunk.data[i]);
	}
	chunk.count = chunk.data[0]->count;
	for (size_t i = 0; i < chunk.colcount; i++) {
		if (chunk.count != chunk.data[i]->count) {
			throw Exception("Projection count mismatch!");
		}
	}
}

unique_ptr<PhysicalOperatorState> PhysicalProjection::GetOperatorState() {
	return make_unique<PhysicalProjectionOperatorState>(
	    children.size() == 0 ? nullptr : children[0].get());
}
