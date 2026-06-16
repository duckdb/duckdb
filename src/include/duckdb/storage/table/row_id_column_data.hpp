//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_id_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/default_virtual_column_data.hpp"

namespace duckdb {

//! Synthetic, not-stored rowid column. Inherits the default-deny base (every ColumnData op throws) and
//! overrides only the producers / scan bookkeeping.
class RowIdColumnData : public DefaultVirtualColumnData {
public:
	RowIdColumnData(BlockManager &block_manager, DataTableInfo &info);

public:
	void InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) override;
	void InitializeScan(ColumnScanState &state) override;
	void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) override;

	idx_t Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	           idx_t scan_count) override;
	void ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t count, Vector &result) override;
	idx_t ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset = 0) override;

	void Filter(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	            SelectionVector &sel, idx_t &count, const TableFilter &filter, TableFilterState &filter_state) override;
	void Select(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	            SelectionVector &sel, idx_t count) override;

	void FetchRows(TransactionData transaction, ColumnFetchState &state, const StorageIndex &storage_index,
	               const idx_t *offsets, const SelectionVector &sel, idx_t count, Vector &result,
	               idx_t result_offset) override;

	void Skip(ColumnScanState &state, idx_t count = STANDARD_VECTOR_SIZE) override;

	FilterPropagateResult CheckZonemap(ColumnScanState &state, TableFilter &filter) override;

	string GetColumnDataName() const override;

	idx_t GetRowStart(ColumnScanState &state);
};

} // namespace duckdb
