#include "duckdb/main/chunk_scan_state/batched_data_collection.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

BatchCollectionChunkScanState::BatchCollectionChunkScanState(BatchedDataCollection &collection,
                                                             BatchedChunkIteratorRange &range, ClientContext &context)
    : ChunkScanState(), collection(collection) {
	collection.InitializeScan(state, range);
	current_chunk = make_uniq<DataChunk>();
	auto &allocator = BufferManager::GetBufferManager(context).GetBufferAllocator();
	current_chunk->Initialize(allocator, collection.Types());
}

BatchCollectionChunkScanState::~BatchCollectionChunkScanState() {
}

void BatchCollectionChunkScanState::InternalLoad(ErrorData &error) {
	if (state.range.begin == state.range.end) {
		// Signal empty chunk to break out of the loop
		current_chunk->SetCardinality(0);
		return;
	}
	offset = 0;
	current_chunk->Reset();
	collection.Scan(state, *current_chunk);
	return;
}

bool BatchCollectionChunkScanState::HasError() const {
	return false;
}

ErrorData &BatchCollectionChunkScanState::GetError() {
	throw NotImplementedException("BatchDataCollections don't have an internal error object");
}

const vector<LogicalType> &BatchCollectionChunkScanState::Types() const {
	return collection.Types();
}

const vector<string> &BatchCollectionChunkScanState::Names() const {
	throw NotImplementedException("BatchDataCollections don't have names");
}

bool BatchCollectionChunkScanState::LoadNextChunk(ErrorData &error) {
	if (finished) {
		return false;
	}
	InternalLoad(error);
	if (ChunkIsEmpty()) {
		finished = true;
	}
	return true;
}

} // namespace duckdb
