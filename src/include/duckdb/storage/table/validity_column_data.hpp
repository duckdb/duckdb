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
	ValidityColumnData(DatabaseInstance &db, DataTableInfo &table_info, idx_t column_idx);

public:
	bool CheckZonemap(ColumnScanState &state, TableFilter &filter) override;
	void InitializeScan(ColumnScanState &state) override;
	void InitializeScanWithOffset(ColumnScanState &state, idx_t vector_idx) override;
	void Scan(Transaction &transaction, ColumnScanState &state, Vector &result) override;
	void IndexScan(ColumnScanState &state, Vector &result, bool allow_pending_updates) override;
	void Update(Transaction &transaction, Vector &updates, Vector &row_ids, idx_t count) override;

	static unique_ptr<PersistentColumnData> Deserialize(DatabaseInstance &db, Deserializer &source);
};

} // namespace duckdb
