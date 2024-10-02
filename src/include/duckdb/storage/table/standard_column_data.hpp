//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/standard_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/validity_column_data.hpp"

namespace duckdb {

//! Standard column data represents a regular flat column (e.g. a column of type INTEGER or STRING)
class StandardColumnData : public ColumnData {
public:
	StandardColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
	                   LogicalType type, optional_ptr<ColumnData> parent = nullptr);

	//! The validity column data
	ValidityColumnData validity;

public:
	void SetStart(idx_t new_start) override;

	ScanVectorType GetVectorScanType(ColumnScanState &state, idx_t scan_count, Vector &result) override;
	void InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) override;
	void InitializeScan(ColumnScanState &state) override;
	void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) override;

	idx_t Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	           idx_t target_count) override;
	idx_t ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
	                    idx_t target_count) override;
	idx_t ScanCount(ColumnScanState &state, Vector &result, idx_t count) override;

	void InitializeAppend(ColumnAppendState &state) override;
	void AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata, idx_t count) override;
	void RevertAppend(row_t start_row) override;
	idx_t Fetch(ColumnScanState &state, row_t row_id, Vector &result) override;
	void FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
	              idx_t result_idx) override;
	void Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
	            idx_t update_count) override;
	void UpdateColumn(TransactionData transaction, const vector<column_t> &column_path, Vector &update_vector,
	                  row_t *row_ids, idx_t update_count, idx_t depth) override;
	unique_ptr<BaseStatistics> GetUpdateStatistics() override;

	void CommitDropColumn() override;

	unique_ptr<ColumnCheckpointState> CreateCheckpointState(RowGroup &row_group,
	                                                        PartialBlockManager &partial_block_manager) override;
	unique_ptr<ColumnCheckpointState> Checkpoint(RowGroup &row_group, ColumnCheckpointInfo &info) override;
	void CheckpointScan(ColumnSegment &segment, ColumnScanState &state, idx_t row_group_start, idx_t count,
	                    Vector &scan_vector) override;

	void GetColumnSegmentInfo(duckdb::idx_t row_group_index, vector<duckdb::idx_t> col_path,
	                          vector<duckdb::ColumnSegmentInfo> &result) override;

	bool IsPersistent() override;
	PersistentColumnData Serialize() override;
	void InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) override;

	void Verify(RowGroup &parent) override;
};

} // namespace duckdb
