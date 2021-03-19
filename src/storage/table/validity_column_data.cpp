#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"

namespace duckdb {

ValidityColumnData::ValidityColumnData(DatabaseInstance &db, DataTableInfo &table_info, idx_t column_idx) :
	ColumnData(db, table_info, LogicalType(LogicalTypeId::VALIDITY), column_idx) {}

bool ValidityColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	return true;
}

void ValidityColumnData::InitializeScan(ColumnScanState &state) {
	state.current = (ColumnSegment *)data.GetRootSegment();
	state.updates = (UpdateSegment *)updates.GetRootSegment();
	state.vector_index = 0;
	state.vector_index_updates = 0;
	state.initialized = false;
}

void ValidityColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t vector_idx) {
	idx_t row_idx = vector_idx * STANDARD_VECTOR_SIZE;
	state.current = (ColumnSegment *)data.GetSegment(row_idx);
	state.updates = (UpdateSegment *)updates.GetSegment(row_idx);
	state.vector_index = (row_idx - state.current->start) / STANDARD_VECTOR_SIZE;
	state.vector_index_updates = (row_idx - state.updates->start) / STANDARD_VECTOR_SIZE;
	state.initialized = false;
}

void ValidityColumnData::Scan(Transaction &transaction, ColumnScanState &state, Vector &result) {
	if (!state.initialized) {
		state.current->InitializeScan(state);
		state.initialized = true;
	}
	// perform a scan of this segment
	state.current->Scan(state, state.vector_index, result);

	// merge the updates into the result
	state.updates->FetchUpdates(transaction, state.vector_index_updates, result);
}

void ValidityColumnData::IndexScan(ColumnScanState &state, Vector &result, bool allow_pending_updates) {
	if (!state.initialized) {
		state.current->InitializeScan(state);
		state.initialized = true;
	}
	state.current->Scan(state, state.vector_index, result);
	if (!allow_pending_updates && state.updates->HasUncommittedUpdates(state.vector_index)) {
		throw TransactionException("Cannot create index with outstanding updates");
	}
	state.updates->FetchCommitted(state.vector_index_updates, result);
}

void ValidityColumnData::Update(Transaction &transaction, Vector &updates, Vector &row_ids, idx_t count) {
	throw NotImplementedException("FIXME: validity update");
}

void ValidityColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw NotImplementedException("FIXME: validity fetch");
}

void ValidityColumnData::FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result, idx_t result_idx) {
	throw NotImplementedException("FIXME: validity fetch row");
}


}
