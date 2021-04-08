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
	//! The maximum amount of tuples that will fit in this segment
	idx_t max_tuple_count;

public:
	void InitializeScan(ColumnScanState &state) override;

	//! Scans a vector of "scan_count" entries starting at position "start"
	//! Store it in result with offset "result_offset"
	void Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset) override;

	//! Fetch a single value and append it to the vector
	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) override;

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats
	//! in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is
	//! full.
	idx_t Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) override;

public:
	typedef void (*append_function_t)(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset,
	                                  VectorData &source, idx_t offset, idx_t count);

private:
	append_function_t append_function;
};

} // namespace duckdb
