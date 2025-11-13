//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/variant_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/validity_column_data.hpp"

namespace duckdb {

//! Struct column data represents a struct
class VariantColumnData : public ColumnData {
public:
	VariantColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, LogicalType type,
	                  ColumnDataType data_type, optional_ptr<ColumnData> parent);

	//! The sub-columns of the struct
	vector<unique_ptr<ColumnData>> sub_columns;
	ValidityColumnData validity;
	//! Whether (some of) the fields are stored outside of the VARIANT data
	bool is_shredded = false;

public:
	idx_t GetMaxEntry() override;

	void InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) override;
	void InitializeScan(ColumnScanState &state) override;
	void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) override;

	Vector CreateUnshreddingIntermediate(idx_t count);
	idx_t Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	           idx_t scan_count) override;
	idx_t ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
	                    idx_t scan_count) override;
	idx_t ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset = 0) override;

	void Skip(ColumnScanState &state, idx_t count = STANDARD_VECTOR_SIZE) override;

	void InitializeAppend(ColumnAppendState &state) override;
	void Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) override;
	void RevertAppend(row_t new_count) override;
	idx_t Fetch(ColumnScanState &state, row_t row_id, Vector &result) override;
	void FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
	              idx_t result_idx) override;
	void Update(TransactionData transaction, DataTable &data_table, idx_t column_index, Vector &update_vector,
	            row_t *row_ids, idx_t update_count, idx_t row_group_start) override;
	void UpdateColumn(TransactionData transaction, DataTable &data_table, const vector<column_t> &column_path,
	                  Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth,
	                  idx_t row_group_start) override;
	unique_ptr<BaseStatistics> GetUpdateStatistics() override;

	void CommitDropColumn() override;

	unique_ptr<ColumnCheckpointState> CreateCheckpointState(RowGroup &row_group,
	                                                        PartialBlockManager &partial_block_manager) override;
	unique_ptr<ColumnCheckpointState> Checkpoint(RowGroup &row_group, ColumnCheckpointInfo &info) override;

	bool IsPersistent() override;
	bool HasAnyChanges() const override;
	PersistentColumnData Serialize() override;
	void InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) override;

	void GetColumnSegmentInfo(const QueryContext &context, duckdb::idx_t row_group_index,
	                          vector<duckdb::idx_t> col_path, vector<duckdb::ColumnSegmentInfo> &result) override;

	void Verify(RowGroup &parent) override;

	static void ShredVariantData(Vector &input, Vector &output, idx_t count);
	static void UnshredVariantData(Vector &input, Vector &output, idx_t count);

private:
	vector<unique_ptr<ColumnData>> WriteShreddedData(RowGroup &row_group, const LogicalType &shredded_type);
	void ReplaceColumns(unique_ptr<ColumnData> &&unshredded, unique_ptr<ColumnData> &&shredded);
	void CreateScanStates(ColumnScanState &state);
	LogicalType GetShreddedType();
};

} // namespace duckdb
