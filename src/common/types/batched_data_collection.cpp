#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

BatchedDataCollection::BatchedDataCollection(vector<LogicalType> types_p) : types(move(types_p)) {
}

void BatchedDataCollection::Append(DataChunk &input, idx_t batch_index) {
	D_ASSERT(batch_index != DConstants::INVALID_INDEX);
	ColumnDataCollection *collection;
	if (last_collection.collection && last_collection.batch_index == batch_index) {
		// we are inserting into the same collection as before: use it directly
		collection = last_collection.collection;
	} else {
		// new collection: check if there is already an entry
		D_ASSERT(data.find(batch_index) == data.end());
		unique_ptr<ColumnDataCollection> new_collection;
		if (last_collection.collection) {
			new_collection = make_unique<ColumnDataCollection>(*last_collection.collection);
		} else {
			new_collection = make_unique<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
		}
		last_collection.collection = new_collection.get();
		last_collection.batch_index = batch_index;
		new_collection->InitializeAppend(last_collection.append_state);
		collection = new_collection.get();
		data.insert(make_pair(batch_index, move(new_collection)));
	}
	collection->Append(last_collection.append_state, input);
}

void BatchedDataCollection::Merge(BatchedDataCollection &other) {
	for (auto &entry : other.data) {
		if (data.find(entry.first) != data.end()) {
			throw InternalException(
			    "BatchedDataCollection::Merge error - batch index %d is present in both collections. This occurs when "
			    "batch indexes are not uniquely distributed over threads",
			    entry.first);
		}
		data[entry.first] = move(entry.second);
	}
	other.data.clear();
}

void BatchedDataCollection::InitializeScan(BatchedChunkScanState &state) {
	state.iterator = data.begin();
	if (state.iterator == data.end()) {
		return;
	}
	state.iterator->second->InitializeScan(state.scan_state);
}

void BatchedDataCollection::Scan(BatchedChunkScanState &state, DataChunk &output) {
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

unique_ptr<ColumnDataCollection> BatchedDataCollection::FetchCollection() {
	unique_ptr<ColumnDataCollection> result;
	for (auto &entry : data) {
		if (!result) {
			result = move(entry.second);
		} else {
			result->Combine(*entry.second);
		}
	}
	data.clear();
	if (!result) {
		// empty result
		return make_unique<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	}
	return result;
}

string BatchedDataCollection::ToString() const {
	string result;
	result += "Batched Data Collection\n";
	for (auto &entry : data) {
		result += "Batch Index - " + to_string(entry.first) + "\n";
		result += entry.second->ToString() + "\n\n";
	}
	return result;
}

void BatchedDataCollection::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb
