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
	StandardColumnData(DatabaseInstance &db, idx_t start_row, LogicalType type, ColumnData *parent = nullptr);

	//! The validity column data
	ValidityColumnData validity;

public:
	bool CheckZonemap(ColumnScanState &state, TableFilter &filter) override;
	void InitializeScan(ColumnScanState &state) override;
	void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) override;
	void Scan(ColumnScanState &state, Vector &result) override;
	void InitializeAppend(ColumnAppendState &state) override;
	void AppendData(BaseStatistics &stats, ColumnAppendState &state, VectorData &vdata, idx_t count) override;
	void RevertAppend(row_t start_row) override;
	void Fetch(ColumnScanState &state, row_t row_id, Vector &result) override;
	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) override;

	void CommitDropColumn() override;
	void Initialize(PersistentColumnData &column_data) override;

	unique_ptr<ColumnCheckpointState> CreateCheckpointState(RowGroup &row_group, TableDataWriter &writer) override;
	unique_ptr<ColumnCheckpointState> Checkpoint(RowGroup &row_group, TableDataWriter &writer,
	                                             idx_t column_idx) override;
	static shared_ptr<ColumnData> Deserialize(DatabaseInstance &db, idx_t start_row, Deserializer &source,
	                                          const LogicalType &type);
};

} // namespace duckdb
