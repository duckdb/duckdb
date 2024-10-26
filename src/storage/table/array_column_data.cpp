#include "duckdb/storage/table/array_column_data.hpp"
#include "duckdb/storage/statistics/array_stats.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

ArrayColumnData::ArrayColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
                                 LogicalType type_p, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, start_row, std::move(type_p), parent),
      validity(block_manager, info, 0, start_row, *this) {
	D_ASSERT(type.InternalType() == PhysicalType::ARRAY);
	auto &child_type = ArrayType::GetChildType(type);
	// the child column, with column index 1 (0 is the validity mask)
	child_column = ColumnData::CreateColumnUnique(block_manager, info, 1, start_row, child_type, this);
}

void ArrayColumnData::SetStart(idx_t new_start) {
	this->start = new_start;
	child_column->SetStart(new_start);
	validity.SetStart(new_start);
}

FilterPropagateResult ArrayColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	// FIXME: There is nothing preventing us from supporting this, but it's not implemented yet.
	// table filters are not supported yet for fixed size list columns
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

void ArrayColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) {
	ColumnData::InitializePrefetch(prefetch_state, scan_state, rows);
	validity.InitializePrefetch(prefetch_state, scan_state.child_states[0], rows);
	auto array_size = ArrayType::GetSize(type);
	child_column->InitializePrefetch(prefetch_state, scan_state.child_states[1], rows * array_size);
}

void ArrayColumnData::InitializeScan(ColumnScanState &state) {
	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 2);

	state.row_index = 0;
	state.current = nullptr;

	validity.InitializeScan(state.child_states[0]);

	// initialize the child scan
	child_column->InitializeScan(state.child_states[1]);
}

void ArrayColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	D_ASSERT(state.child_states.size() == 2);

	if (row_idx == 0) {
		// Trivial case, no offset
		InitializeScan(state);
		return;
	}

	state.row_index = row_idx;
	state.current = nullptr;

	// initialize the validity segment
	validity.InitializeScanWithOffset(state.child_states[0], row_idx);

	auto array_size = ArrayType::GetSize(type);
	auto child_count = (row_idx - start) * array_size;

	D_ASSERT(child_count <= child_column->GetMaxEntry());
	if (child_count < child_column->GetMaxEntry()) {
		const auto child_offset = start + child_count;
		child_column->InitializeScanWithOffset(state.child_states[1], child_offset);
	}
}

idx_t ArrayColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                            idx_t scan_count) {
	return ScanCount(state, result, scan_count);
}

idx_t ArrayColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
                                     idx_t scan_count) {
	return ScanCount(state, result, scan_count);
}

idx_t ArrayColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	// Scan validity
	auto scan_count = validity.ScanCount(state.child_states[0], result, count);
	auto array_size = ArrayType::GetSize(type);
	// Scan child column
	auto &child_vec = ArrayVector::GetEntry(result);
	child_column->ScanCount(state.child_states[1], child_vec, count * array_size);
	return scan_count;
}

void ArrayColumnData::Skip(ColumnScanState &state, idx_t count) {
	// Skip validity
	validity.Skip(state.child_states[0], count);
	// Skip child column
	auto array_size = ArrayType::GetSize(type);
	child_column->Skip(state.child_states[1], count * array_size);
}

void ArrayColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnAppendState validity_append;
	validity.InitializeAppend(validity_append);
	state.child_appends.push_back(std::move(validity_append));

	ColumnAppendState child_append;
	child_column->InitializeAppend(child_append);
	state.child_appends.push_back(std::move(child_append));
}

void ArrayColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	vector.Flatten(count);
	// Append validity
	validity.Append(stats, state.child_appends[0], vector, count);
	// Append child column
	auto array_size = ArrayType::GetSize(type);
	auto &child_vec = ArrayVector::GetEntry(vector);
	child_column->Append(ArrayStats::GetChildStats(stats), state.child_appends[1], child_vec, count * array_size);

	this->count += count;
}

void ArrayColumnData::RevertAppend(row_t start_row) {
	// Revert validity
	validity.RevertAppend(start_row);
	// Revert child column
	auto array_size = ArrayType::GetSize(type);
	child_column->RevertAppend(start_row * UnsafeNumericCast<row_t>(array_size));

	this->count = UnsafeNumericCast<idx_t>(start_row) - this->start;
}

idx_t ArrayColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw NotImplementedException("Array Fetch");
}

void ArrayColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                             idx_t update_count) {
	throw NotImplementedException("Array Update is not supported.");
}

void ArrayColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path,
                                   Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	throw NotImplementedException("Array Update Column is not supported");
}

unique_ptr<BaseStatistics> ArrayColumnData::GetUpdateStatistics() {
	return nullptr;
}

void ArrayColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                               idx_t result_idx) {

	// Create state for validity & child column
	if (state.child_states.empty()) {
		state.child_states.push_back(make_uniq<ColumnFetchState>());
	}

	// Fetch validity
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);

	// Fetch child column
	auto &child_vec = ArrayVector::GetEntry(result);
	auto &child_type = ArrayType::GetChildType(type);
	auto array_size = ArrayType::GetSize(type);

	// We need to fetch between [row_id * array_size, (row_id + 1) * array_size)
	auto child_state = make_uniq<ColumnScanState>();
	child_state->Initialize(child_type, nullptr);

	const auto child_offset = start + (UnsafeNumericCast<idx_t>(row_id) - start) * array_size;

	child_column->InitializeScanWithOffset(*child_state, child_offset);
	Vector child_scan(child_type, array_size);
	child_column->ScanCount(*child_state, child_scan, array_size);
	VectorOperations::Copy(child_scan, child_vec, array_size, 0, result_idx * array_size);
}

void ArrayColumnData::CommitDropColumn() {
	validity.CommitDropColumn();
	child_column->CommitDropColumn();
}

struct ArrayColumnCheckpointState : public ColumnCheckpointState {
	ArrayColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		global_stats = ArrayStats::CreateEmpty(column_data.type).ToUnique();
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	unique_ptr<ColumnCheckpointState> child_state;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		auto stats = global_stats->Copy();
		ArrayStats::SetChildStats(stats, child_state->GetStatistics());
		return stats.ToUnique();
	}

	PersistentColumnData ToPersistentData() override {
		PersistentColumnData data(PhysicalType::ARRAY);
		data.child_columns.push_back(validity_state->ToPersistentData());
		data.child_columns.push_back(child_state->ToPersistentData());
		return data;
	}
};

unique_ptr<ColumnCheckpointState> ArrayColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                         PartialBlockManager &partial_block_manager) {
	return make_uniq<ArrayColumnCheckpointState>(row_group, *this, partial_block_manager);
}

unique_ptr<ColumnCheckpointState> ArrayColumnData::Checkpoint(RowGroup &row_group,
                                                              ColumnCheckpointInfo &checkpoint_info) {

	auto checkpoint_state = make_uniq<ArrayColumnCheckpointState>(row_group, *this, checkpoint_info.info.manager);
	checkpoint_state->validity_state = validity.Checkpoint(row_group, checkpoint_info);
	checkpoint_state->child_state = child_column->Checkpoint(row_group, checkpoint_info);
	return std::move(checkpoint_state);
}

bool ArrayColumnData::IsPersistent() {
	return validity.IsPersistent() && child_column->IsPersistent();
}

PersistentColumnData ArrayColumnData::Serialize() {
	PersistentColumnData persistent_data(PhysicalType::ARRAY);
	persistent_data.child_columns.push_back(validity.Serialize());
	persistent_data.child_columns.push_back(child_column->Serialize());
	return persistent_data;
}

void ArrayColumnData::InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) {
	D_ASSERT(column_data.pointers.empty());
	validity.InitializeColumn(column_data.child_columns[0], target_stats);
	auto &child_stats = ArrayStats::GetChildStats(target_stats);
	child_column->InitializeColumn(column_data.child_columns[1], child_stats);
	this->count = validity.count.load();
}

void ArrayColumnData::GetColumnSegmentInfo(idx_t row_group_index, vector<idx_t> col_path,
                                           vector<ColumnSegmentInfo> &result) {
	col_path.push_back(0);
	validity.GetColumnSegmentInfo(row_group_index, col_path, result);
	col_path.back() = 1;
	child_column->GetColumnSegmentInfo(row_group_index, col_path, result);
}

void ArrayColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);
	validity.Verify(parent);
	child_column->Verify(parent);
#endif
}

} // namespace duckdb
