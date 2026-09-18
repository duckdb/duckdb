#include "duckdb/common/column_data_collection_render_interface.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

ColumnDataCollectionWrapper::ColumnDataCollectionWrapper(const ColumnDataCollection &collection)
    : collection(collection) {
}

const vector<LogicalType> &ColumnDataCollectionWrapper::Types() const {
	return collection.Types();
}

idx_t ColumnDataCollectionWrapper::Count() const {
	return collection.Count();
}

idx_t ColumnDataCollectionWrapper::ColumnCount() const {
	return collection.ColumnCount();
}

idx_t ColumnDataCollectionWrapper::ChunkCount() const {
	return collection.ChunkCount();
}

void ColumnDataCollectionWrapper::FetchChunk(idx_t chunk_idx, DataChunk &result) const {
	collection.FetchChunk(chunk_idx, result);
}

} // namespace duckdb
