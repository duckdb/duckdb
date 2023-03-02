#include "duckdb/common/types/row/tuple_data_iterator.hpp"

namespace duckdb {

TupleDataChunkIterator::TupleDataChunkIterator(TupleDataCollection &collection_p, TupleDataPinProperties properties_p)
    : TupleDataChunkIterator(collection_p, properties_p, 0, collection_p.ChunkCount()) {
}

TupleDataChunkIterator::TupleDataChunkIterator(TupleDataCollection &collection_p, TupleDataPinProperties properties_p,
                                               idx_t chunk_idx_from, idx_t chunk_idx_to)
    : collection(collection_p), properties(properties_p) {
	D_ASSERT(start_chunk_idx < end_chunk_idx);
	D_ASSERT(end_chunk_idx < collection.ChunkCount());
	idx_t overall_chunk_index = 0;
	for (idx_t segment_idx = 0; segment_idx < collection.segments.size(); segment_idx++) {
		const auto &segment = collection.segments[segment_idx];
		if (chunk_idx_from >= overall_chunk_index && chunk_idx_from <= overall_chunk_index + segment.ChunkCount()) {
			// We start in this segment
			start_segment_idx = segment_idx;
			start_chunk_idx = start_chunk_idx - overall_chunk_index;
		}
		if (chunk_idx_to >= overall_chunk_index && chunk_idx_to <= overall_chunk_index + segment.ChunkCount()) {
			// We end in this segment
			end_segment_idx = segment_idx;
			end_chunk_idx = end_chunk_idx - overall_chunk_index;
		}
		overall_chunk_index += segment.ChunkCount();
	}

	current_segment_idx = start_segment_idx;
	current_chunk_idx = start_chunk_idx;
	InitializeCurrentChunk();
}

void TupleDataChunkIterator::InitializeCurrentChunk() {
	collection.allocator->InitializeChunkState(state, collection.segments[current_segment_idx], current_chunk_idx,
	                                           properties);
}

bool TupleDataChunkIterator::Next() {
	// Check if we were already done
	if (current_segment_idx == end_segment_idx && current_chunk_idx == end_chunk_idx) {
		return false;
	}

	// Increment indices
	if (++current_chunk_idx == collection.segments[current_segment_idx].ChunkCount()) {
		current_segment_idx++;
		current_chunk_idx = 0;
	}

	// Check if we are done (again)
	if (current_segment_idx == end_segment_idx && current_chunk_idx == end_chunk_idx) {
		return false;
	}

	InitializeCurrentChunk();
	return true;
}

void TupleDataChunkIterator::Reset() {
	current_segment_idx = start_segment_idx;
	current_chunk_idx = start_chunk_idx;
	InitializeCurrentChunk();
}

idx_t TupleDataChunkIterator::GetCount() const {
	return collection.segments[current_segment_idx].chunks[current_chunk_idx].count;
}

data_ptr_t *TupleDataChunkIterator::GetRowLocations() {
	return FlatVector::GetData<data_ptr_t>(state.row_locations);
}

} // namespace duckdb
