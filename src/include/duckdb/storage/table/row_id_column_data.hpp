//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_id_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

class RowIdColumnData : public ColumnData {
public:
	RowIdColumnData(BlockManager &block_manager, DataTableInfo &info);

public:
	void InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) override;
	void InitializeScan(ColumnScanState &state) override;
	void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) override;

	idx_t Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	           idx_t scan_count) override;
	idx_t ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
	                    idx_t scan_count) override;
	void ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t count, Vector &result) override;
	idx_t ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset = 0) override;

	void Filter(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	            SelectionVector &sel, idx_t &count, const TableFilter &filter, TableFilterState &filter_state) override;
	void Select(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	            SelectionVector &sel, idx_t count) override;
	void SelectCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t count,
	                     bool allow_updates) override;

	idx_t Fetch(ColumnScanState &state, row_t row_id, Vector &result) override;
	void FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
	              idx_t result_idx) override;

	void Skip(ColumnScanState &state, idx_t count = STANDARD_VECTOR_SIZE) override;

	FilterPropagateResult CheckZonemap(ColumnScanState &state, TableFilter &filter) override;

	void InitializeAppend(ColumnAppendState &state) override;
	void Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) override;
	void AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata, idx_t count) override;
	void RevertAppend(row_t new_count) override;

	void Update(TransactionData transaction, DataTable &data_table, idx_t column_index, Vector &update_vector,
	            row_t *row_ids, idx_t update_count, idx_t row_group_start) override;
	void UpdateColumn(TransactionData transaction, DataTable &data_table, const vector<column_t> &column_path,
	                  Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth,
	                  idx_t row_group_start) override;

	void CommitDropColumn() override;

	unique_ptr<ColumnCheckpointState> CreateCheckpointState(const RowGroup &row_group,
	                                                        PartialBlockManager &partial_block_manager) override;
	unique_ptr<ColumnCheckpointState> Checkpoint(const RowGroup &row_group, ColumnCheckpointInfo &info) override;

	void CheckpointScan(ColumnSegment &segment, ColumnScanState &state, idx_t count,
	                    Vector &scan_vector) const override;

	bool IsPersistent() override;
};

} // namespace duckdb
