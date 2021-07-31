//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/segment/compressed_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {
class BlockHandle;
class ColumnData;
class ConstantFilter;
class Transaction;
class StorageManager;
class TableFilter;

struct ColumnAppendState;
struct UpdateInfo;

class CompressedSegment {
public:
	CompressedSegment(ColumnSegment *parent, DatabaseInstance &db, PhysicalType type, idx_t row_start, CompressionFunction *function, block_id_t block_id = INVALID_BLOCK);

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
	//! The compression function
	CompressionFunction *function;
	//! The parent column segment of the compressed segment
	ColumnSegment *parent;

public:
	void InitializeScan(ColumnScanState &state);

	void Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result);
	void ScanPartial(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result,
	                 idx_t result_offset);
	static void FilterSelection(SelectionVector &sel, Vector &result, const TableFilter &filter,
	                            idx_t &approved_tuple_count, ValidityMask &mask);

	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx);

	idx_t Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count);
	void RevertAppend(idx_t start_row);

	bool RowIdIsValid(idx_t row_id) const;
	bool RowRangeIsValid(idx_t row_id, idx_t count) const;
	CompressedSegmentState *GetSegmentState() {
		return segment_state.get();
	}

private:
	unique_ptr<CompressedSegmentState> segment_state;
};

} // namespace duckdb
