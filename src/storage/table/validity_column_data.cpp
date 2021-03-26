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

void ValidityColumnData::Update(Transaction &transaction, Vector &update_vector, Vector &row_ids, idx_t count) {
	idx_t first_id = FlatVector::GetValue<row_t>(row_ids, 0);

	// fetch the validity data for this segment
	Vector base_data(LogicalType::BOOLEAN, nullptr);
	auto column_segment = (ColumnSegment *)data.GetSegment(first_id);
	auto vector_index = (first_id - column_segment->start) / STANDARD_VECTOR_SIZE;
	// now perform the fetch within the segment
	ColumnScanState state;
	column_segment->Fetch(state, vector_index, base_data);

	// first find the segment that the update belongs to
	auto segment = (UpdateSegment *)updates.GetSegment(first_id);
	// now perform the update within the segment
	segment->Update(transaction, update_vector, FlatVector::GetData<row_t>(row_ids), count, base_data);
}

unique_ptr<PersistentColumnData> ValidityColumnData::Deserialize(DatabaseInstance &db, Deserializer &source) {
	auto result = make_unique<PersistentColumnData>();
	BaseDeserialize(db, source, LogicalType(LogicalTypeId::VALIDITY), *result);
	return result;
}

} // namespace duckdb
