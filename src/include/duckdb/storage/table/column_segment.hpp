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

namespace duckdb {
class BlockManager;
class ColumnSegment;
class ColumnData;
class Transaction;
class TableFilter;
struct ColumnFetchState;
struct ColumnScanState;
enum class ColumnSegmentType : uint8_t { TRANSIENT, PERSISTENT };
//! TableFilter represents a filter pushed down into the table scan.

class SegmentStatistics {
public:
	SegmentStatistics(TypeId type, idx_t type_size);
	SegmentStatistics(TypeId type, idx_t type_size, data_t stats_min[], data_t stats_max[]);
	TypeId type;
	idx_t type_size;
	//! The minimum value of the segment
	unique_ptr<data_t[]> minimum;
	//! The maximum value of the segment
	unique_ptr<data_t[]> maximum;
	//! Whether or not the segment has NULL values
	bool has_null;
	//! The maximum string length, only used for string columns
	idx_t max_string_length;
	//! Whether or not the segment contains any big strings in overflow blocks, only used for string columns
	bool has_overflow_strings;

public:
	void Reset();
};

class ColumnSegment : public SegmentBase {
public:
	//! Initialize an empty column segment of the specified type
	ColumnSegment(TypeId type, ColumnSegmentType segment_type, idx_t start, idx_t count = 0);

	ColumnSegment(TypeId type, ColumnSegmentType segment_type, idx_t start, idx_t count, data_t stats_min[],
	              data_t stats_max[]);

	virtual ~ColumnSegment() = default;

	//! The type stored in the column
	TypeId type;
	//! The size of the type
	idx_t type_size;
	//! The column segment type (transient or persistent)
	ColumnSegmentType segment_type;
	//! The statistics for the segment
	SegmentStatistics stats;

public:
	virtual void InitializeScan(ColumnScanState &state) = 0;
	//! Scan one vector from this segment
	virtual void Scan(Transaction &transaction, ColumnScanState &state, idx_t vector_index, Vector &result) = 0;
	//! Scan the next vector from the column and apply a selection vector to filter the data
	virtual void FilterScan(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
	                        idx_t &approved_tuple_count) = 0;
	//! Scan one vector from this segment, throwing an exception if there are any outstanding updates
	virtual void IndexScan(ColumnScanState &state, Vector &result) = 0;
	//! Executes filter in this column
	virtual void Select(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
	                    idx_t &approved_tuple_count, vector<TableFilter> &tableFilter) = 0;
	//! Fetch the base table vector index that belongs to this row
	virtual void Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) = 0;
	//! Fetch a value of the specific row id and append it to the result
	virtual void FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
	                      idx_t result_idx) = 0;

	//! Perform an update within the segment
	virtual void Update(ColumnData &column_data, Transaction &transaction, Vector &updates, row_t *ids,
	                    idx_t count) = 0;
};

} // namespace duckdb
