
#include "execution/operator/physical_projection.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

vector<TypeId> PhysicalProjection::GetTypes() {
	// get the chunk types from the projection list
	vector<TypeId> types;
	for (auto &expr : select_list) {
		types.push_back(expr->return_type);
	}
	return types;
}

void PhysicalProjection::GetChunk(DataChunk &chunk,
                                  PhysicalOperatorState *state) {
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
		if (state->finished) {
			return;
		}
		state->finished = true;
	}

	ExpressionExecutor executor(state);

	for (size_t i = 0; i < select_list.size(); i++) {
		auto &expr = select_list[i];
		executor.Execute(expr.get(), *chunk.data[i]);
	}
	chunk.count = chunk.data[0]->count;
	for (size_t i = 0; i < chunk.column_count; i++) {
		if (chunk.count != chunk.data[i]->count) {
			throw Exception("Projection count mismatch!");
		}
	}
}

unique_ptr<PhysicalOperatorState>
PhysicalProjection::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalOperatorState>(
	    children.size() == 0 ? nullptr : children[0].get(), parent_executor);
}
