//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/block.hpp"
#include "duckdb/storage/table/segment_tree.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/storage/uncompressed_segment.hpp"

namespace duckdb {
class BlockManager;
class ColumnSegment;
class ColumnData;
class DatabaseInstance;
class Transaction;
class BaseStatistics;
class UncompressedSegment;
class UpdateSegment;
class TableFilter;
struct ColumnFetchState;
struct ColumnScanState;

enum class ColumnSegmentType : uint8_t { TRANSIENT, PERSISTENT };
//! TableFilter represents a filter pushed down into the table scan.

class ColumnSegment : public SegmentBase {
public:
	//! Initialize an empty column segment of the specified type
	ColumnSegment(DatabaseInstance &db, LogicalType type, ColumnSegmentType segment_type, idx_t start, idx_t count = 0);

	ColumnSegment(DatabaseInstance &db, LogicalType type, ColumnSegmentType segment_type, idx_t start, idx_t count,
	              unique_ptr<BaseStatistics> statistics);
	~ColumnSegment() override;

	//! The database instance
	DatabaseInstance &db;
	//! The type stored in the column
	LogicalType type;
	//! The size of the type
	idx_t type_size;
	//! The column segment type (transient or persistent)
	ColumnSegmentType segment_type;
	//! The statistics for the segment
	SegmentStatistics stats;
	//! The uncompressed segment holding the data
	unique_ptr<UncompressedSegment> data;

public:
	void InitializeScan(ColumnScanState &state);
	//! Scan one vector from this segment
	void Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset,
	          bool entire_vector);
	//! Fetch a value of the specific row id and append it to the result
	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx);
};

} // namespace duckdb
