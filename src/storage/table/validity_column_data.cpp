#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"

namespace duckdb {

ValidityColumnData::ValidityColumnData(DatabaseInstance &db, DataTableInfo &table_info, idx_t column_idx)
    : ColumnData(db, table_info, LogicalType(LogicalTypeId::VALIDITY), column_idx) {
}

bool ValidityColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	return true;
}

void ValidityColumnData::InitializeScan(ColumnScanState &state) {
	state.current = (ColumnSegment *)data.GetRootSegment();
	state.row_index = 0;
	state.initialized = false;
}

void ValidityColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t vector_idx) {
	idx_t row_idx = vector_idx * STANDARD_VECTOR_SIZE;
	state.current = (ColumnSegment *)data.GetSegment(row_idx);
	state.row_index = row_idx;
	state.initialized = false;
}

void ValidityColumnData::Scan(Transaction &transaction, ColumnScanState &state, Vector &result) {
	if (!state.initialized) {
		state.current->InitializeScan(state);
		state.initialized = true;
	}
	// perform a scan of this segment
	ScanVector(transaction, state, result);
}

void ValidityColumnData::IndexScan(ColumnScanState &state, Vector &result, bool allow_pending_updates) {
	if (!state.initialized) {
		state.current->InitializeScan(state);
		state.initialized = true;
	}
	if (!allow_pending_updates && state.current->updates && state.current->updates->HasUncommittedUpdates(state.row_index)) {
		throw TransactionException("Cannot create index with outstanding updates");
	}
	ScanCommitted(state, result);
}

unique_ptr<PersistentColumnData> ValidityColumnData::Deserialize(DatabaseInstance &db, Deserializer &source) {
	auto result = make_unique<PersistentColumnData>();
	BaseDeserialize(db, source, LogicalType(LogicalTypeId::VALIDITY), *result);
	return result;
}

} // namespace duckdb
