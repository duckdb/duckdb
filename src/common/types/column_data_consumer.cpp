#include "duckdb/common/types/column_data_consumer.hpp"

namespace duckdb {

using ChunkReferenceState = ColumnDataConsumer::ChunkReferenceState;
using ChunkReference = ColumnDataConsumer::ChunkReference;

ColumnDataConsumer::ColumnDataConsumer(ColumnDataCollection &collection_p) : collection(collection_p) {
}

void ColumnDataConsumer::InitializeScan() {
	chunk_count = collection.ChunkCount();
	// Initialize chunk references and sort them so we can scan them in a sane order
	chunk_references.reserve(chunk_count);
	for (auto &segment : collection.GetSegments()) {
		for (idx_t chunk_index = 0; chunk_index < segment->chunk_data.size(); chunk_index++) {
			chunk_references.emplace_back(segment.get(), chunk_index);
		}
	}
	std::sort(chunk_references.begin(), chunk_references.end());
	current_chunk_index = 0;
	minimum_chunk_index_in_progress = DConstants::INVALID_INDEX;
}

bool ColumnDataConsumer::AssignChunk(idx_t &assigned_chunk_index) {
	lock_guard<mutex> guard(lock);
	if (current_chunk_index == chunk_count) {
		return false;
	}
	assigned_chunk_index = current_chunk_index++;
	auto &chunk_ref = chunk_references[assigned_chunk_index];
	chunk_ref.state = ChunkReferenceState::IN_PROGRESS;
	return true;
}

void ColumnDataConsumer::Scan(ColumnDataConsumerLocalState &state, idx_t chunk_index, DataChunk &chunk) {
	auto &chunk_ref = chunk_references[chunk_index];
}

} // namespace duckdb