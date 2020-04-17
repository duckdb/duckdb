#include "duckdb/execution/operator/helper/physical_limit.hpp"

using namespace duckdb;
using namespace std;

class PhysicalLimitOperatorState : public PhysicalOperatorState {
public:
	PhysicalLimitOperatorState(PhysicalOperator *child, idx_t current_offset = 0)
	    : PhysicalOperatorState(child), current_offset(current_offset) {
	}

	idx_t current_offset;
};

void PhysicalLimit::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalLimitOperatorState *>(state_);

	idx_t max_element = limit + offset;
	if (state->current_offset >= max_element) {
		return;
	}

	// get the next chunk from the child
	do {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}

		if (state->current_offset < offset) {
			// we are not yet at the offset point
			if (state->current_offset + state->child_chunk.size() > offset) {
				// however we will reach it in this chunk
				// we have to copy part of the chunk with an offset
				idx_t start_position = offset - state->current_offset;
				idx_t chunk_count = min(limit, state->child_chunk.size() - start_position);
				SelectionVector sel(STANDARD_VECTOR_SIZE);
				for (idx_t i = 0; i < chunk_count; i++) {
					sel.set_index(i, start_position + i);
				}
				// set up a slice of the input chunks
				chunk.Slice(state->child_chunk, sel, chunk_count);
			}
		} else {
			// have to copy either the entire chunk or part of it
			idx_t chunk_count;
			if (state->current_offset + state->child_chunk.size() >= max_element) {
				// have to limit the count of the chunk
				chunk_count = max_element - state->current_offset;
			} else {
				// we copy the entire chunk
				chunk_count = state->child_chunk.size();
			}
			// instead of copying we just change the pointer in the current chunk
			chunk.Reference(state->child_chunk);
			chunk.SetCardinality(chunk_count);
		}

		state->current_offset += state->child_chunk.size();
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalLimit::GetOperatorState() {
	return make_unique<PhysicalLimitOperatorState>(children[0].get(), 0);
}
