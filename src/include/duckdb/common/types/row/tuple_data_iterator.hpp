//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_collection.hpp"

namespace duckdb {

class TupleDataChunkIterator {
public:
	TupleDataChunkIterator(TupleDataCollection &collection, TupleDataPinProperties properties);
	TupleDataChunkIterator(TupleDataCollection &collection, TupleDataPinProperties properties, idx_t chunk_idx_from,
	                       idx_t chunk_idx_to);

public:
	bool Next();
	void Reset();
	idx_t GetCount() const;
	data_ptr_t *GetRowLocations();

private:
	void InitializeCurrentChunk();

private:
	TupleDataCollection &collection;
	TupleDataPinProperties properties;
	idx_t start_segment_idx;
	idx_t start_chunk_idx;
	idx_t end_segment_idx;
	idx_t end_chunk_idx;

	TupleDataScanState state;
	idx_t current_segment_idx;
	idx_t current_chunk_idx;
};

} // namespace duckdb
