//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_number_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

class RowNumberColumnData : public ColumnData {
public:
	RowNumberColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t start_row);

public:
	void InitializeScan(ColumnScanState &state) override;
	void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) override;
	idx_t Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	           idx_t scan_count) override;
	idx_t ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
	                    idx_t scan_count) override;
	void ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t count, Vector &result) override;
	idx_t ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset = 0) override;
};

} // namespace duckdb
