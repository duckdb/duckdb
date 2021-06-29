//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/constant_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/uncompressed_segment.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class DatabaseInstance;
class SegmentStatistics;

class ConstantSegment : public UncompressedSegment {
public:
	ConstantSegment(DatabaseInstance &db, SegmentStatistics &stats, idx_t row_start);

	SegmentStatistics &stats;
public:
	void InitializeScan(ColumnScanState &state) override;

	void Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset) override;

	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) override;

	idx_t Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) override;

public:
	typedef void (*scan_function_t)(ConstantSegment &segment, Vector &result);
	typedef void (*fetch_function_t)(ConstantSegment &segment, Vector &result, idx_t result_idx);

private:
	scan_function_t scan_function;
	fetch_function_t fetch_function;
};

} // namespace duckdb
