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
class BlockHandle;
class ColumnData;
class Transaction;
class StorageManager;

struct ColumnAppendState;
struct UpdateInfo;

//! An uncompressed segment represents an uncompressed segment of a column residing in a block
class UncompressedSegment {
public:
	UncompressedSegment(DatabaseInstance &db, PhysicalType type, idx_t row_start);
	virtual ~UncompressedSegment();

	//! The storage manager
	DatabaseInstance &db;
	//! Type of the uncompressed segment
	PhysicalType type;
	//! The block that this segment relates to
	shared_ptr<BlockHandle> block;
	//! The size of a vector of this type
	idx_t vector_size;
	//! The maximum amount of vectors that can be stored in this segment
	idx_t max_vector_count;
	//! The current amount of tuples that are stored in this segment
	atomic<idx_t> tuple_count;
	//! The starting row of this segment
	const idx_t row_start;

public:
	virtual void InitializeScan(ColumnScanState &state) {
	}
	//! Fetch the vector at index "vector_index" from the uncompressed segment, storing it in the result vector
	void Scan(ColumnScanState &state, idx_t vector_index, Vector &result);

	static void FilterSelection(SelectionVector &sel, Vector &result, const TableFilter &filter,
	                            idx_t &approved_tuple_count, ValidityMask &mask);

	//! Fetch a single vector from the base table
	void Fetch(ColumnScanState &state, idx_t vector_index, Vector &result);
	//! Fetch a single value and append it to the vector
	virtual void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) = 0;

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats
	//! in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is
	//! full.
	virtual idx_t Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) = 0;
	//! Truncate a previous append
	virtual void RevertAppend(idx_t start_row);

	//! Convert a persistently backed uncompressed segment (i.e. one where block_id refers to an on-disk block) to a
	//! temporary in-memory one
	virtual void ToTemporary();
	void ToTemporaryInternal();

	//! Get the amount of tuples in a vector
	idx_t GetVectorCount(idx_t vector_index) {
		D_ASSERT(vector_index < max_vector_count);
		D_ASSERT(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);
		return MinValue<idx_t>(STANDARD_VECTOR_SIZE, tuple_count - vector_index * STANDARD_VECTOR_SIZE);
	}

	virtual void Verify();

protected:
	//! Fetch base table data
	virtual void FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) = 0;
};

} // namespace duckdb
