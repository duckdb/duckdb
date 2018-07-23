
#include "execution/operator/physical_limit.hpp"

using namespace duckdb;
using namespace std;

void PhysicalLimit::InitializeChunk(DataChunk &chunk) {
	// just copy the chunk data of the child
	children[0]->InitializeChunk(chunk);
}

void PhysicalLimit::GetChunk(DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalLimitOperatorState *>(state_);
	chunk.Reset();

	size_t max_element = limit + offset;
	if (state->current_offset >= max_element) {
		return;
	}

	// get the next chunk from the child
	children[0]->GetChunk(state->child_chunk, state->child_state.get());
	if (state->child_chunk.count == 0) {
		return;
	}

	if (state->current_offset < offset) {
		// we are not yet at the offset point
		if (state->current_offset + state->child_chunk.count >= offset) {
			// however we will reach it in this chunk
			// we have to copy part of the chunk with an offset
			throw NotImplementedException("TODO: handle offset in limit");
		}
	} else {
		// have to copy either the entire chunk or part of it
		if (state->current_offset + state->child_chunk.count >= max_element) {
			// have to limit the count of the chunk
			chunk.count = max_element - state->current_offset;
		} else {
			// we copy the entire chunk
			chunk.count = state->child_chunk.count;
		}
		// instead of copying we just change the pointer in the current chunk
		for (size_t i = 0; i < chunk.column_count; i++) {
			chunk.data[i]->data = state->child_chunk.data[i]->data;
			chunk.data[i]->owns_data = false;
			chunk.data[i]->count = chunk.count;
		}
	}

	state->current_offset += state->child_chunk.count;
}

unique_ptr<PhysicalOperatorState> PhysicalLimit::GetOperatorState() {
	return make_unique<PhysicalLimitOperatorState>(children[0].get());
}
