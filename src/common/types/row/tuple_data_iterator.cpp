#include "duckdb/common/types/row/tuple_data_iterator.hpp"

namespace duckdb {

TupleDataChunkIterator::TupleDataChunkIterator(TupleDataCollection &collection_p, TupleDataPinProperties properties_p,
                                               bool init_heap)
    : TupleDataChunkIterator(collection_p, properties_p, 0, collection_p.ChunkCount(), init_heap) {
}

TupleDataChunkIterator::TupleDataChunkIterator(TupleDataCollection &collection_p, TupleDataPinProperties properties,
                                               idx_t chunk_idx_from, idx_t chunk_idx_to, bool init_heap_p)
    : collection(collection_p), init_heap(init_heap_p) {
	state.chunk_state.properties = properties;
	D_ASSERT(chunk_idx_from < chunk_idx_to);
	D_ASSERT(chunk_idx_to <= collection.ChunkCount());
	idx_t overall_chunk_index = 0;
	for (idx_t segment_idx = 0; segment_idx < collection.segments.size(); segment_idx++) {
		const auto &segment = collection.segments[segment_idx];
		if (chunk_idx_from >= overall_chunk_index && chunk_idx_from <= overall_chunk_index + segment.ChunkCount()) {
			// We start in this segment
			start_segment_idx = segment_idx;
			start_chunk_idx = chunk_idx_from - overall_chunk_index;
		}
		if (chunk_idx_to >= overall_chunk_index && chunk_idx_to <= overall_chunk_index + segment.ChunkCount()) {
			// We end in this segment
			end_segment_idx = segment_idx;
			end_chunk_idx = chunk_idx_to - overall_chunk_index;
		}
		overall_chunk_index += segment.ChunkCount();
	}

	Reset();
}

void TupleDataChunkIterator::InitializeCurrentChunk() {
	auto &segment = collection.segments[current_segment_idx];
	segment.allocator->InitializeChunkState(state.chunk_state, segment, current_chunk_idx, init_heap);
}

bool TupleDataChunkIterator::Next() {
	// Check if called after already done
	D_ASSERT(current_segment_idx != end_segment_idx || current_chunk_idx != end_chunk_idx);

	// Set the next indices and checks if we're at the end of the collection
	// NextScanIndex can go past this iterators 'end', so we have to check the indices again
	if (!collection.NextScanIndex(state, current_segment_idx, current_chunk_idx) ||
	    (current_segment_idx == end_segment_idx && current_chunk_idx == end_chunk_idx)) {
		// Drop pins / store them if TupleDataPinProperties::KEEP_EVERYTHING_PINNED
		collection.FinalizeChunkState(state.chunk_state);
		return false;
	}

	InitializeCurrentChunk();
	return true;
}

void TupleDataChunkIterator::Reset() {
	state.segment_index = start_segment_idx;
	state.chunk_index = start_chunk_idx;
	collection.NextScanIndex(state, current_segment_idx, current_chunk_idx);
	InitializeCurrentChunk();
}

idx_t TupleDataChunkIterator::GetCount() const {
	return collection.segments[current_segment_idx].chunks[current_chunk_idx].count;
}

data_ptr_t *TupleDataChunkIterator::GetRowLocations() {
	return FlatVector::GetData<data_ptr_t>(state.chunk_state.row_locations);
}

} // namespace duckdb
