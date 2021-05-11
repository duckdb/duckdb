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
	//! The current amount of tuples that are stored in this segment
	atomic<idx_t> tuple_count;
	//! The starting row of this segment
	const idx_t row_start;

public:
	virtual void InitializeScan(ColumnScanState &state) {
	}
	//! Scans a vector of "scan_count" entries starting at position "start"
	//! Store it in result with offset "result_offset"
	virtual void Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset) = 0;

	static void FilterSelection(SelectionVector &sel, Vector &result, const TableFilter &filter,
	                            idx_t &approved_tuple_count, ValidityMask &mask);

	//! Fetch a single value and append it to the vector
	virtual void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) = 0;

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats
	//! in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is
	//! full.
	virtual idx_t Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) = 0;
	//! Truncate a previous append
	virtual void RevertAppend(idx_t start_row);

	virtual void Verify();

	bool RowIdIsValid(idx_t row_id) const;
	bool RowRangeIsValid(idx_t row_id, idx_t count) const;
};

} // namespace duckdb
