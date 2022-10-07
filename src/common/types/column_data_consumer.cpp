#include "duckdb/common/types/column_data_consumer.hpp"

namespace duckdb {

ColumnDataConsumer::ColumnDataConsumer(ColumnDataCollection &collection_p, vector<column_t> column_ids)
    : collection(collection_p), column_ids(move(column_ids)) {
}

void ColumnDataConsumer::InitializeScan() {
	chunk_count = collection.ChunkCount();
	current_chunk_index = 0;
	current_chunk_index = 0;

	// Initialize chunk references and sort them, so we can scan them in a sane order, regardless of how it was created
	chunk_references.reserve(chunk_count);
	for (auto &segment : collection.GetSegments()) {
		for (idx_t chunk_index = 0; chunk_index < segment->chunk_data.size(); chunk_index++) {
			chunk_references.emplace_back(segment.get(), chunk_index);
		}
	}
	std::sort(chunk_references.begin(), chunk_references.end());
}

bool ColumnDataConsumer::AssignChunk(ColumnDataConsumerScanState &state) {
	lock_guard<mutex> guard(lock);
	if (current_chunk_index == chunk_count) {
		// All chunks have been assigned
		state.chunk_index = DConstants::INVALID_INDEX;
		return false;
	}
	// Assign chunk index
	state.chunk_index = current_chunk_index++;
	D_ASSERT(chunks_in_progress.find(state.chunk_index) == chunks_in_progress.end());
	chunks_in_progress.insert(state.chunk_index);
	return true;
}

void ColumnDataConsumer::ScanChunk(ColumnDataConsumerScanState &state, DataChunk &chunk) const {
	D_ASSERT(state.chunk_index < chunk_count);
	auto &chunk_ref = chunk_references[state.chunk_index];
	if (state.allocator != chunk_ref.segment->allocator.get()) {
		// Previously scanned a chunk from a different allocator, reset the handles
		state.allocator = chunk_ref.segment->allocator.get();
		state.current_chunk_state.handles.clear();
	}
	chunk_ref.segment->ReadChunk(chunk_ref.chunk_index_in_segment, state.current_chunk_state, chunk, column_ids);
}

void ColumnDataConsumer::FinishChunk(ColumnDataConsumerScanState &state) {
	D_ASSERT(state.chunk_index < chunk_count);
	idx_t delete_index_start;
	idx_t delete_index_end;
	{
		lock_guard<mutex> guard(lock);
		D_ASSERT(chunks_in_progress.find(state.chunk_index) != chunks_in_progress.end());
		delete_index_start = chunk_delete_index;
		delete_index_end = *std::min_element(chunks_in_progress.begin(), chunks_in_progress.end());
		chunks_in_progress.erase(state.chunk_index);
		chunk_delete_index = delete_index_end;
	}
	ConsumeChunks(delete_index_start, delete_index_end);
}
void ColumnDataConsumer::ConsumeChunks(idx_t delete_index_start, idx_t delete_index_end) {
	for (idx_t chunk_index = delete_index_start; chunk_index < delete_index_end; chunk_index++) {
		if (chunk_index == 0) {
			continue;
		}
		auto &prev_chunk_ref = chunk_references[chunk_index - 1];
		auto &curr_chunk_ref = chunk_references[chunk_index];
		if (prev_chunk_ref.segment->allocator.get() != curr_chunk_ref.segment->allocator.get()) {
			// Moved to the next allocator, reset the previous one
			prev_chunk_ref.segment->allocator->Reset();
			continue;
		}
		// Same allocator, see if we can delete blocks
		auto prev_min_block_id = prev_chunk_ref.GetMinimumBlockID();
		auto curr_min_block_id = curr_chunk_ref.GetMinimumBlockID();
		for (idx_t block_id = prev_min_block_id; block_id < curr_min_block_id; block_id++) {
			prev_chunk_ref.segment->allocator->DeleteBlock(block_id);
		}
	}
}

} // namespace duckdb
