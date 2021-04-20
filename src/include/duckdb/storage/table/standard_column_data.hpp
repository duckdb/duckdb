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
	StandardColumnData(Morsel &morsel, LogicalType type, idx_t column_idx, ColumnData *parent = nullptr);

	//! The validity column data
	ValidityColumnData validity;

public:
	bool CheckZonemap(ColumnScanState &state, TableFilter &filter) override;
	void InitializeScan(ColumnScanState &state) override;
	void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) override;
	void Scan(ColumnScanState &state, Vector &result) override;
	void InitializeAppend(ColumnAppendState &state) override;
	void AppendData(ColumnAppendState &state, VectorData &vdata, idx_t count) override;
	void RevertAppend(row_t start_row) override;
	void Fetch(ColumnScanState &state, row_t row_id, Vector &result) override;
	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) override;

	unique_ptr<BaseStatistics> GetStatistics() override;

	void CommitDropColumn() override;
	void Initialize(PersistentColumnData &column_data) override;
	void Checkpoint(TableDataWriter &writer) override;
	static unique_ptr<PersistentColumnData> Deserialize(DatabaseInstance &db, Deserializer &source,
	                                                    const LogicalType &type);
};

} // namespace duckdb
