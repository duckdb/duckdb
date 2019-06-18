#include "execution/operator/helper/physical_limit.hpp"

using namespace duckdb;
using namespace std;

void PhysicalLimit::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalLimitOperatorState *>(state_);

	index_t max_element = limit + offset;
	if (state->current_offset >= max_element) {
		return;
	}

	// get the next chunk from the child
	children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
	if (state->child_chunk.size() == 0) {
		return;
	}

	if (state->current_offset < offset) {
		// we are not yet at the offset point
		if (state->current_offset + state->child_chunk.size() >= offset) {
			// however we will reach it in this chunk
			// we have to copy part of the chunk with an offset
			index_t start_position = offset - state->current_offset;
			index_t chunk_count = min(limit, state->child_chunk.size() - start_position);
			for (index_t i = 0; i < chunk.column_count; i++) {
				chunk.data[i].Reference(state->child_chunk.data[i]);
				chunk.data[i].data = chunk.data[i].data + GetTypeIdSize(chunk.data[i].type) * start_position;
				chunk.data[i].count = chunk_count;
			}
			chunk.sel_vector = move(state->child_chunk.sel_vector);
		}
	} else {
		// have to copy either the entire chunk or part of it
		index_t chunk_count;
		if (state->current_offset + state->child_chunk.size() >= max_element) {
			// have to limit the count of the chunk
			chunk_count = max_element - state->current_offset;
		} else {
			// we copy the entire chunk
			chunk_count = state->child_chunk.size();
		}
		// instead of copying we just change the pointer in the current chunk
		for (index_t i = 0; i < chunk.column_count; i++) {
			chunk.data[i].Reference(state->child_chunk.data[i]);
			chunk.data[i].count = chunk_count;
		}
		chunk.sel_vector = move(state->child_chunk.sel_vector);
	}

	state->current_offset += state->child_chunk.size();
}

unique_ptr<PhysicalOperatorState> PhysicalLimit::GetOperatorState() {
	return make_unique<PhysicalLimitOperatorState>(children[0].get(), 0);
}
