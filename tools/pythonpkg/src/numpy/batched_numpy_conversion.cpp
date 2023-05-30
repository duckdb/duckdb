#include "duckdb_python/numpy/batched_numpy_conversion.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

BatchedNumpyConversion::BatchedNumpyConversion(vector<LogicalType> types_p) : types(std::move(types_p)) {
}

void BatchedNumpyConversion::Append(DataChunk &input, idx_t batch_index) {
	D_ASSERT(batch_index != DConstants::INVALID_INDEX);
	optional_ptr<NumpyResultConversion> collection;
	if (last_collection.collection && last_collection.batch_index == batch_index) {
		// we are inserting into the same collection as before: use it directly
		collection = last_collection.collection;
	} else {
		// new collection
		py::gil_scoped_acquire gil;
		D_ASSERT(data.find(batch_index) == data.end());
		unique_ptr<NumpyResultConversion> new_collection;
		new_collection = make_uniq<NumpyResultConversion>(types, input.size());

		last_collection.collection = new_collection.get();
		last_collection.batch_index = batch_index;
		collection = new_collection.get();
		data.insert(make_pair(batch_index, std::move(new_collection)));
	}
	collection->Append(input);
}

void BatchedNumpyConversion::Merge(BatchedNumpyConversion &other) {
	for (auto &entry : other.data) {
		if (data.find(entry.first) != data.end()) {
			throw InternalException(
			    "BatchedNumpyConversion::Merge error - batch index %d is present in both collections. This occurs when "
			    "batch indexes are not uniquely distributed over threads",
			    entry.first);
		}
		data[entry.first] = std::move(entry.second);
	}
	other.data.clear();
}

unique_ptr<NumpyResultConversion> BatchedNumpyConversion::FetchCollection() {
	unique_ptr<NumpyResultConversion> collection;

	D_ASSERT(!data.empty());
	if (data.size() == 1) {
		auto entry = data.begin();
		collection = std::move(entry->second);
	} else {
		vector<unique_ptr<NumpyResultConversion>> batches;
		batches.reserve(data.size());
		for (auto &entry : data) {
			batches.push_back(std::move(entry.second));
		}
		py::gil_scoped_acquire gil;
		collection = make_uniq<NumpyResultConversion>(std::move(batches), types);
	}
	data.clear();
	return collection;
}

string BatchedNumpyConversion::ToString() const {
	return "";
}

void BatchedNumpyConversion::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb
