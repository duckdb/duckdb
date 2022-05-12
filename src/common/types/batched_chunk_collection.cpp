#include "duckdb/common/types/batched_chunk_collection.hpp"

namespace duckdb {

void BatchedChunkCollection::Append(DataChunk &input, idx_t batch_index) {
	D_ASSERT(batch_index != DConstants::INVALID_INDEX);
	auto entry = data.find(batch_index);
	ChunkCollection *collection;
	if (entry == data.end()) {
		auto new_collection = make_unique<ChunkCollection>();
		collection = new_collection.get();
		data.insert(make_pair(batch_index, move(new_collection)));
	} else {
		collection = entry->second.get();
	}
	collection->Append(input);
}

void BatchedChunkCollection::Merge(BatchedChunkCollection &other) {
	for(auto &entry : other.data) {
		if (data.find(entry.first) != data.end()) {
			throw InternalException("BatchChunkCollection::Merge error - batch index %d is present in both collections. This occurs when batch indexes are not uniquely distributed over threads", entry.first);
		}
		data[entry.first] = move(entry.second);
	}
	other.data.clear();
}

void BatchedChunkCollection::InitializeScan(BatchedChunkScanState &state) {
	state.iterator = data.begin();
	state.chunk_index = 0;
}

void BatchedChunkCollection::Scan(BatchedChunkScanState &state, DataChunk &output) {
	while (state.iterator != data.end()) {
		// check if there is a chunk remaining in this collection
		auto collection = state.iterator->second.get();
		if (state.chunk_index < collection->ChunkCount()) {
			// there is! increment the chunk count
			output.Reference(collection->GetChunk(state.chunk_index));
			state.chunk_index++;
			return;
		}
		// there isn't! move to the next collection
		state.iterator++;
		state.chunk_index = 0;
	}
}

}
