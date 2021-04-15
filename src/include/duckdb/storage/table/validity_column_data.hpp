//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/validity_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/column_data.hpp"

namespace duckdb {

//! Validity column data represents the validity data (i.e. which values are null)
class ValidityColumnData : public ColumnData {
public:
	ValidityColumnData(DatabaseInstance &db, DataTableInfo &table_info, idx_t column_idx, ColumnData *parent);

public:
	bool CheckZonemap(ColumnScanState &state, TableFilter &filter) override;
	void InitializeScan(ColumnScanState &state) override;
	void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) override;
	void Scan(Transaction &transaction, ColumnScanState &state, Vector &result) override;
	void IndexScan(ColumnScanState &state, Vector &result, bool allow_pending_updates) override;

	static unique_ptr<PersistentColumnData> Deserialize(DatabaseInstance &db, Deserializer &source);
};

} // namespace duckdb
