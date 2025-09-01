#include "duckdb/storage/table/row_id_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"

namespace duckdb {

RowIdColumnData::RowIdColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t start_row)
    : ColumnData(block_manager, info, COLUMN_IDENTIFIER_ROW_ID, start_row, LogicalType(LogicalTypeId::BIGINT),
                 nullptr) {
}

FilterPropagateResult RowIdColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	return RowGroup::CheckRowIdFilter(filter, start, start + count);
	;
}

void RowIdColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) {
}

void RowIdColumnData::InitializeScan(ColumnScanState &state) {
	InitializeScanWithOffset(state, start);
}

void RowIdColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	state.current = nullptr;
	state.segment_tree = nullptr;
	state.row_index = row_idx;
	state.internal_index = state.row_index;
	state.initialized = true;
	state.scan_state.reset();
	state.last_offset = 0;
}

idx_t RowIdColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                            idx_t scan_count) {
	return ScanCommitted(vector_index, state, result, true, scan_count);
}

idx_t RowIdColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
                                     idx_t scan_count) {
	return ScanCount(state, result, scan_count, 0);
}

void RowIdColumnData::ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t count,
                                         Vector &result) {
	D_ASSERT(this->start == row_group_start);
	result.Sequence(UnsafeNumericCast<int64_t>(this->start + offset_in_row_group), 1, count);
}

idx_t RowIdColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset) {
	if (result_offset != 0) {
		throw InternalException("RowIdColumnData result_offset must be 0");
	}
	ScanCommittedRange(start, state.row_index - start, count, result);
	state.row_index += count;
	return count;
}

void RowIdColumnData::Filter(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                             SelectionVector &sel, idx_t &count, const TableFilter &filter,
                             TableFilterState &filter_state) {
	auto current_row = state.row_index;
	auto max_count = GetVectorCount(vector_index);
	state.row_index += max_count;
	// We do another quick statistics scan for row ids here
	const auto rowid_start = current_row;
	const auto rowid_end = current_row + max_count;
	const auto prune_result = RowGroup::CheckRowIdFilter(filter, rowid_start, rowid_end);
	if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
		// We can just break out of the loop here.
		count = 0;
		return;
	}

	// Generate row ids
	// Create sequence for row ids
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<row_t>(result);
	for (size_t sel_idx = 0; sel_idx < count; sel_idx++) {
		result_data[sel.get_index(sel_idx)] = UnsafeNumericCast<int64_t>(current_row + sel.get_index(sel_idx));
	}

	// Was this filter always true? If so, we dont need to apply it
	if (prune_result == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
		return;
	}

	// Now apply the filter
	UnifiedVectorFormat vdata;
	result.ToUnifiedFormat(count, vdata);
	ColumnSegment::FilterSelection(sel, result, vdata, filter, filter_state, count, count);
}

void RowIdColumnData::Select(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                             SelectionVector &sel, idx_t count) {
	SelectCommitted(vector_index, state, result, sel, count, true);
}

void RowIdColumnData::SelectCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, SelectionVector &sel,
                                      idx_t count, bool allow_updates) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<row_t>(result);
	for (size_t sel_idx = 0; sel_idx < count; sel_idx++) {
		result_data[sel_idx] = UnsafeNumericCast<row_t>(state.row_index + sel.get_index(sel_idx));
	}
	state.row_index += GetVectorCount(vector_index);
}

idx_t RowIdColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw InternalException("Fetch is not supported for row id columns");
}

void RowIdColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                               idx_t result_idx) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto data = FlatVector::GetData<row_t>(result);
	data[result_idx] = row_id;
}
void RowIdColumnData::Skip(ColumnScanState &state, idx_t count) {
	state.row_index += count;
	state.internal_index = state.row_index;
}

void RowIdColumnData::InitializeAppend(ColumnAppendState &state) {
	throw InternalException("RowIdColumnData cannot be appended to");
}

void RowIdColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	throw InternalException("RowIdColumnData cannot be appended to");
}

void RowIdColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata,
                                 idx_t count) {
	throw InternalException("RowIdColumnData cannot be appended to");
}

void RowIdColumnData::RevertAppend(row_t start_row) {
	throw InternalException("RowIdColumnData cannot be appended to");
}

void RowIdColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                             idx_t update_count) {
	throw InternalException("RowIdColumnData cannot be updated");
}

void RowIdColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path,
                                   Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	throw InternalException("RowIdColumnData cannot be updated");
}

void RowIdColumnData::CommitDropColumn() {
	throw InternalException("RowIdColumnData cannot be dropped");
}

unique_ptr<ColumnCheckpointState> RowIdColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                         PartialBlockManager &partial_block_manager) {
	throw InternalException("RowIdColumnData cannot be checkpointed");
}

unique_ptr<ColumnCheckpointState> RowIdColumnData::Checkpoint(RowGroup &row_group, ColumnCheckpointInfo &info) {
	throw InternalException("RowIdColumnData cannot be checkpointed");
}

void RowIdColumnData::CheckpointScan(ColumnSegment &segment, ColumnScanState &state, idx_t row_group_start, idx_t count,
                                     Vector &scan_vector) {
	throw InternalException("RowIdColumnData cannot be checkpointed");
}

bool RowIdColumnData::IsPersistent() {
	throw InternalException("RowIdColumnData cannot be persisted");
}

} // namespace duckdb
