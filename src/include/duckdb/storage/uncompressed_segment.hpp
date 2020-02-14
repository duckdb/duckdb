//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/uncompressed_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {
class BufferManager;
class ColumnData;
class Transaction;

struct ColumnAppendState;
struct UpdateInfo;

//! An uncompressed segment represents an uncompressed segment of a column residing in a block
class UncompressedSegment {
public:
	UncompressedSegment(BufferManager &manager, TypeId type, index_t row_start);
	virtual ~UncompressedSegment();

	//! The buffer manager
	BufferManager &manager;
	//! Type of the uncompressed segment
	TypeId type;
	//! The block id that this segment relates to
	block_id_t block_id;
	//! The size of a vector of this type
	index_t vector_size;
	//! The maximum amount of vectors that can be stored in this segment
	index_t max_vector_count;
	//! The current amount of tuples that are stored in this segment
	index_t tuple_count;
	//! The starting row of this segment
	index_t row_start;
	//! Version chains for each of the vectors
	unique_ptr<UpdateInfo *[]> versions;
	//! The lock for the uncompressed segment
	StorageLock lock;

public:
	virtual void InitializeScan(ColumnScanState &state) {
	}
	//! Fetch the vector at index "vector_index" from the uncompressed segment, storing it in the result vector
	void Scan(Transaction &transaction, ColumnScanState &state, index_t vector_index, Vector &result);
	//! Fetch the vector at index "vector_index" from the uncompressed segment, throwing an exception if there are any
	//! outstanding updates
	void IndexScan(ColumnScanState &state, index_t vector_index, Vector &result);

	//! Fetch a single vector from the base table
	void Fetch(ColumnScanState &state, index_t vector_index, Vector &result);
	//! Fetch a single value and append it to the vector
	virtual void FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result, index_t result_idx) = 0;

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats
	//! in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is
	//! full.
	virtual index_t Append(SegmentStatistics &stats, Vector &data, index_t offset, index_t count) = 0;

	//! Update a set of row identifiers to the specified set of updated values
	void Update(ColumnData &data, SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids,
	            row_t offset);

	//! Rollback a previous update
	virtual void RollbackUpdate(UpdateInfo *info) = 0;
	//! Cleanup an update, removing it from the version chain. This should only be called if an exclusive lock is held
	//! on the segment
	void CleanupUpdate(UpdateInfo *info);

	//! Convert a persistently backed uncompressed segment (i.e. one where block_id refers to an on-disk block) to a
	//! temporary in-memory one
	void ToTemporary();

	//! Get the amount of tuples in a vector
	index_t GetVectorCount(index_t vector_index) {
		assert(vector_index < max_vector_count);
		assert(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);
		return std::min((index_t)STANDARD_VECTOR_SIZE, tuple_count - vector_index * STANDARD_VECTOR_SIZE);
	}

protected:
	virtual void Update(ColumnData &data, SegmentStatistics &stats, Transaction &transaction, Vector &update,
	                    row_t *ids, index_t vector_index, index_t vector_offset, UpdateInfo *node) = 0;
	//! Fetch base table data
	virtual void FetchBaseData(ColumnScanState &state, index_t vector_index, Vector &result) = 0;
	//! Fetch update data from an UpdateInfo version
	virtual void FetchUpdateData(ColumnScanState &state, Transaction &transaction, UpdateInfo *version,
	                             Vector &result) = 0;

	//! Create a new update info for the specified transaction reflecting an update of the specified rows
	UpdateInfo *CreateUpdateInfo(ColumnData &data, Transaction &transaction, row_t *ids, index_t count,
	                             index_t vector_index, index_t vector_offset, index_t type_size);
};

} // namespace duckdb
