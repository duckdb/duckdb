//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/validity_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/block.hpp"
#include "duckdb/storage/segment/uncompressed_segment.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {
class BlockHandle;
class DatabaseInstance;
class SegmentStatistics;
class Vector;
struct VectorData;

class ValiditySegment : public UncompressedSegment {
	static const validity_t LOWER_MASKS[65];
	static const validity_t UPPER_MASKS[65];

public:
	ValiditySegment(DatabaseInstance &db, idx_t row_start, block_id_t block_id = INVALID_BLOCK);
	~ValiditySegment();

	idx_t max_tuples;

public:
	void InitializeScan(ColumnScanState &state) override;
	void Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result) override;
	void ScanPartial(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result,
	                 idx_t result_offset) override;

	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) override;
	idx_t Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) override;
	void RevertAppend(idx_t start_row) override;
};

} // namespace duckdb
