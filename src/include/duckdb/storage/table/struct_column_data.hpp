//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/struct_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/validity_column_data.hpp"

namespace duckdb {

//! Struct column data represents a struct
class StructColumnData : public ColumnData {
public:
	StructColumnData(DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type,
	                 ColumnData *parent = nullptr);

	//! The sub-columns of the struct
	vector<unique_ptr<ColumnData>> sub_columns;
	//! The validity column data of the struct
	ValidityColumnData validity;

public:
	bool CheckZonemap(ColumnScanState &state, TableFilter &filter) override;
	idx_t GetMaxEntry() override;

	void InitializeScan(ColumnScanState &state) override;
	void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) override;

	idx_t Scan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result) override;
	idx_t ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) override;
	idx_t ScanCount(ColumnScanState &state, Vector &result, idx_t count) override;

	void InitializeAppend(ColumnAppendState &state) override;
	void Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) override;
	void RevertAppend(row_t start_row) override;
	idx_t Fetch(ColumnScanState &state, row_t row_id, Vector &result) override;
	void FetchRow(Transaction &transaction, ColumnFetchState &state, row_t row_id, Vector &result,
	              idx_t result_idx) override;
	void Update(Transaction &transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
	            idx_t update_count) override;
	void UpdateColumn(Transaction &transaction, const vector<column_t> &column_path, Vector &update_vector,
	                  row_t *row_ids, idx_t update_count, idx_t depth) override;
	unique_ptr<BaseStatistics> GetUpdateStatistics() override;

	void CommitDropColumn() override;

	unique_ptr<ColumnCheckpointState> CreateCheckpointState(RowGroup &row_group, TableDataWriter &writer) override;
	unique_ptr<ColumnCheckpointState> Checkpoint(RowGroup &row_group, TableDataWriter &writer,
	                                             ColumnCheckpointInfo &checkpoint_info) override;

	void DeserializeColumn(Deserializer &source) override;

	void GetStorageInfo(idx_t row_group_index, vector<idx_t> col_path, vector<vector<Value>> &result) override;

	void Verify(RowGroup &parent) override;
};

} // namespace duckdb
