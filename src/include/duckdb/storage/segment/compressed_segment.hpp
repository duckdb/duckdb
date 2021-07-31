//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/segment/compressed_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/segment/base_segment.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

class CompressedSegment : public BaseSegment {
public:
	CompressedSegment(DatabaseInstance &db, PhysicalType type, idx_t row_start, CompressionFunction *function, block_id_t block_id = INVALID_BLOCK);

	CompressionFunction *function;

public:
	void InitializeScan(ColumnScanState &state) override;

	void Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result) override;
	void ScanPartial(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result,
	                 idx_t result_offset) override;

	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) override;

	idx_t Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) override;
	void RevertAppend(idx_t start_row) override;

	CompressedSegmentState *GetSegmentState() {
		return segment_state.get();
	}

private:
	unique_ptr<CompressedSegmentState> segment_state;
};

} // namespace duckdb
