#include "duckdb/common/types/batched_chunk_collection.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

BatchedChunkCollection::BatchedChunkCollection(BufferManager &buffer_manager, vector<LogicalType> types_p)
    : buffer_manager(buffer_manager), types(move(types_p)) {
}

BatchedChunkCollection::BatchedChunkCollection(ClientContext &context, vector<LogicalType> types_p)
    : BatchedChunkCollection(BufferManager::GetBufferManager(context), move(types_p)) {
}

void BatchedChunkCollection::Append(DataChunk &input, idx_t batch_index) {
	D_ASSERT(batch_index != DConstants::INVALID_INDEX);
	auto entry = data.find(batch_index);
	ColumnDataCollection *collection;
	if (entry == data.end()) {
		auto new_collection = make_unique<ColumnDataCollection>(buffer_manager, types);
		collection = new_collection.get();
		data.insert(make_pair(batch_index, move(new_collection)));
	} else {
		collection = entry->second.get();
	}
	collection->Append(input);
}

void BatchedChunkCollection::Merge(BatchedChunkCollection &other) {
	for (auto &entry : other.data) {
		if (data.find(entry.first) != data.end()) {
			throw InternalException(
			    "BatchChunkCollection::Merge error - batch index %d is present in both collections. This occurs when "
			    "batch indexes are not uniquely distributed over threads",
			    entry.first);
		}
		data[entry.first] = move(entry.second);
	}
	other.data.clear();
}

void BatchedChunkCollection::InitializeScan(BatchedChunkScanState &state) {
	state.iterator = data.begin();
	if (state.iterator == data.end()) {
		return;
	}
	state.iterator->second->InitializeScan(state.scan_state);
}

void BatchedChunkCollection::Scan(BatchedChunkScanState &state, DataChunk &output) {
	while (state.iterator != data.end()) {
		// check if there is a chunk remaining in this collection
		auto collection = state.iterator->second.get();
		collection->Scan(state.scan_state, output);
		if (output.size() > 0) {
			return;
		}
		// there isn't! move to the next collection
		state.iterator++;
		if (state.iterator == data.end()) {
			return;
		}
		state.iterator->second->InitializeScan(state.scan_state);
	}
}

string BatchedChunkCollection::ToString() const {
	string result;
	result += "Batched Chunk Collection\n";
	for (auto &entry : data) {
		result += "Batch Index - " + to_string(entry.first) + "\n";
		result += entry.second->ToString() + "\n\n";
	}
	return result;
}

void BatchedChunkCollection::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb
