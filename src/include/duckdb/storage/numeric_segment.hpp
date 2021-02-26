//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/numeric_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/uncompressed_segment.hpp"

namespace duckdb {
class DatabaseInstance;

class NumericSegment : public UncompressedSegment {
public:
	NumericSegment(DatabaseInstance &db, PhysicalType type, idx_t row_start, block_id_t block_id = INVALID_BLOCK);

	//! The size of this type
	idx_t type_size;

public:
	void InitializeScan(ColumnScanState &state) override;

	//! Fetch a single value and append it to the vector
	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result,
	              idx_t result_idx) override;

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats
	//! in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is
	//! full.
	idx_t Append(SegmentStatistics &stats, Vector &data, idx_t offset, idx_t count) override;

protected:
	void Select(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count,
	            vector<TableFilter> &table_filter) override;
	void FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) override;
	void FilterFetchBaseData(ColumnScanState &state, Vector &result, SelectionVector &sel,
	                         idx_t &approved_tuple_count) override;

public:
	typedef void (*append_function_t)(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, Vector &source,
	                                  idx_t offset, idx_t count);

private:
	append_function_t append_function;
};

} // namespace duckdb
