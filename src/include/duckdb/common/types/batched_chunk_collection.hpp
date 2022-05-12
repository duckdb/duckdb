//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/batched_chunk_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

namespace duckdb {

struct BatchedChunkScanState {
	map<idx_t, unique_ptr<ChunkCollection>>::iterator iterator;
	idx_t chunk_index;
};

//!  A BatchedChunkCollection holds a number of data entries that are partitioned by batch index
//! Scans over a BatchedChunkCollection are ordered by batch index
class BatchedChunkCollection {
public:
	//! Appends a datachunk with the given batch index to the batched collection
	void Append(DataChunk &input, idx_t batch_index);

	//! Merge the other batched chunk collection into this batched collection
	void Merge(BatchedChunkCollection &other);

	//! Initialize a scan over the batched chunk collection
	void InitializeScan(BatchedChunkScanState &state);

	//! Scan a chunk from the batched chunk collection, in-order of batch index
	void Scan(BatchedChunkScanState &state, DataChunk &output);

private:
	//! The data of the batched chunk collection - a set of batch_index -> ChunkCollection pointers
	map<idx_t, unique_ptr<ChunkCollection>> data;
};
} // namespace duckdb
