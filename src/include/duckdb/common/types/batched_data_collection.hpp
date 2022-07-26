//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/batched_chunk_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/types/column_data_collection.hpp"

namespace duckdb {
class BufferManager;
class ClientContext;

struct BatchedChunkScanState {
	map<idx_t, unique_ptr<ColumnDataCollection>>::iterator iterator;
	ColumnDataScanState scan_state;
};

//!  A BatchedDataCollection holds a number of data entries that are partitioned by batch index
//! Scans over a BatchedDataCollection are ordered by batch index
class BatchedDataCollection {
public:
	DUCKDB_API BatchedDataCollection(vector<LogicalType> types);

	//! Appends a datachunk with the given batch index to the batched collection
	DUCKDB_API void Append(DataChunk &input, idx_t batch_index);

	//! Merge the other batched chunk collection into this batched collection
	DUCKDB_API void Merge(BatchedDataCollection &other);

	//! Initialize a scan over the batched chunk collection
	DUCKDB_API void InitializeScan(BatchedChunkScanState &state);

	//! Scan a chunk from the batched chunk collection, in-order of batch index
	DUCKDB_API void Scan(BatchedChunkScanState &state, DataChunk &output);

	//! Fetch a column data collection from the batched data collection - this consumes all of the data stored within
	DUCKDB_API unique_ptr<ColumnDataCollection> FetchCollection();

	DUCKDB_API string ToString() const;
	DUCKDB_API void Print() const;

private:
	struct CachedCollection {
		idx_t batch_index = DConstants::INVALID_INDEX;
		ColumnDataCollection *collection = nullptr;
		ColumnDataAppendState append_state;
	};

	vector<LogicalType> types;
	//! The data of the batched chunk collection - a set of batch_index -> ColumnDataCollection pointers
	map<idx_t, unique_ptr<ColumnDataCollection>> data;
	//! The last batch collection that was inserted into
	CachedCollection last_collection;
};
} // namespace duckdb
