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

//!  A BatchedChunkCollection holds a number of data entries that are partitioned by batch index
//! Scans over a BatchedChunkCollection are ordered by batch index
class BatchedChunkCollection {
public:
	DUCKDB_API BatchedChunkCollection(BufferManager &buffer_manager, vector<LogicalType> types);
	DUCKDB_API BatchedChunkCollection(ClientContext &context, vector<LogicalType> types);

	//! Appends a datachunk with the given batch index to the batched collection
	DUCKDB_API void Append(DataChunk &input, idx_t batch_index);

	//! Merge the other batched chunk collection into this batched collection
	DUCKDB_API void Merge(BatchedChunkCollection &other);

	//! Initialize a scan over the batched chunk collection
	DUCKDB_API void InitializeScan(BatchedChunkScanState &state);

	//! Scan a chunk from the batched chunk collection, in-order of batch index
	DUCKDB_API void Scan(BatchedChunkScanState &state, DataChunk &output);

	DUCKDB_API string ToString() const;
	DUCKDB_API void Print() const;

private:
	BufferManager &buffer_manager;
	vector<LogicalType> types;
	//! The data of the batched chunk collection - a set of batch_index -> ChunkCollection pointers
	map<idx_t, unique_ptr<ColumnDataCollection>> data;
};
} // namespace duckdb
