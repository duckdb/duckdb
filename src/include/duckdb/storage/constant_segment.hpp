//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/constant_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/base_segment.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class DatabaseInstance;
class SegmentStatistics;

class ConstantSegment : public BaseSegment {
public:
	ConstantSegment(DatabaseInstance &db, SegmentStatistics &stats, idx_t row_start);

	SegmentStatistics &stats;

public:
	void InitializeScan(ColumnScanState &state) override;

	void Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result) override;
	void ScanPartial(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result,
	                 idx_t result_offset) override;

	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) override;

public:
	typedef void (*scan_function_t)(ConstantSegment &segment, Vector &result);
	typedef void (*fill_function_t)(ConstantSegment &segment, Vector &result, idx_t start_idx, idx_t count);

private:
	scan_function_t scan_function;
	fill_function_t fill_function;
};

} // namespace duckdb
