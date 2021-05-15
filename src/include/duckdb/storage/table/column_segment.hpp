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

namespace duckdb {
class BlockManager;
class ColumnSegment;
class ColumnData;
class Transaction;
class BaseStatistics;
struct ColumnFetchState;
struct ColumnScanState;
enum class ColumnSegmentType : uint8_t { TRANSIENT, PERSISTENT };
//! TableFilter represents a filter pushed down into the table scan.

class ColumnSegment : public SegmentBase {
public:
	//! Initialize an empty column segment of the specified type
	ColumnSegment(LogicalType type, ColumnSegmentType segment_type, idx_t start, idx_t count = 0);

	ColumnSegment(LogicalType type, ColumnSegmentType segment_type, idx_t start, idx_t count,
	              unique_ptr<BaseStatistics> statistics);

	~ColumnSegment() override = default;

	//! The type stored in the column
	LogicalType type;
	//! The size of the type
	idx_t type_size;
	//! The column segment type (transient or persistent)
	ColumnSegmentType segment_type;
	//! The statistics for the segment
	SegmentStatistics stats;

public:
	virtual void InitializeScan(ColumnScanState &state) = 0;
	//! Scan one vector from this segment
	virtual void Scan(ColumnScanState &state, idx_t vector_index, Vector &result) = 0;
	//! Fetch the base table vector index that belongs to this row
	virtual void Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) = 0;
	//! Fetch a value of the specific row id and append it to the result
	virtual void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) = 0;
};

} // namespace duckdb
