#include "duckdb/storage/table/struct_column_data.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"

namespace duckdb {

StructColumnData::StructColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                   idx_t start_row, LogicalType type_p, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, start_row, std::move(type_p), parent),
      validity(block_manager, info, 0, start_row, *this) {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(!child_types.empty());
	if (type.id() != LogicalTypeId::UNION && StructType::IsUnnamed(type)) {
		throw InvalidInputException("A table cannot be created from an unnamed struct");
	}
	// the sub column index, starting at 1 (0 is the validity mask)
	idx_t sub_column_index = 1;
	for (auto &child_type : child_types) {
		sub_columns.push_back(
		    ColumnData::CreateColumnUnique(block_manager, info, sub_column_index, start_row, child_type.second, this));
		sub_column_index++;
	}
}

void StructColumnData::SetStart(idx_t new_start) {
	this->start = new_start;
	for (auto &sub_column : sub_columns) {
		sub_column->SetStart(new_start);
	}
	validity.SetStart(new_start);
}

idx_t StructColumnData::GetMaxEntry() {
	return sub_columns[0]->GetMaxEntry();
}

void StructColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) {
	validity.InitializePrefetch(prefetch_state, scan_state.child_states[0], rows);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		if (!scan_state.scan_child_column[i]) {
			continue;
		}
		sub_columns[i]->InitializePrefetch(prefetch_state, scan_state.child_states[i + 1], rows);
	}
}

void StructColumnData::InitializeScan(ColumnScanState &state) {
	D_ASSERT(state.child_states.size() == sub_columns.size() + 1);
	state.row_index = 0;
	state.current = nullptr;

	// initialize the validity segment
	validity.InitializeScan(state.child_states[0]);

	// initialize the sub-columns
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		if (!state.scan_child_column[i]) {
			continue;
		}
		sub_columns[i]->InitializeScan(state.child_states[i + 1]);
	}
}

void StructColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	D_ASSERT(state.child_states.size() == sub_columns.size() + 1);
	state.row_index = row_idx;
	state.current = nullptr;

	// initialize the validity segment
	validity.InitializeScanWithOffset(state.child_states[0], row_idx);

	// initialize the sub-columns
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		if (!state.scan_child_column[i]) {
			continue;
		}
		sub_columns[i]->InitializeScanWithOffset(state.child_states[i + 1], row_idx);
	}
}

idx_t StructColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                             idx_t target_count) {
	auto scan_count = validity.Scan(transaction, vector_index, state.child_states[0], result, target_count);
	auto &child_entries = StructVector::GetEntries(result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto &target_vector = *child_entries[i];
		if (!state.scan_child_column[i]) {
			// if we are not scanning this vector - set it to NULL
			target_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(target_vector, true);
			continue;
		}
		sub_columns[i]->Scan(transaction, vector_index, state.child_states[i + 1], target_vector, target_count);
	}
	return scan_count;
}

idx_t StructColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
                                      idx_t target_count) {
	auto scan_count = validity.ScanCommitted(vector_index, state.child_states[0], result, allow_updates, target_count);
	auto &child_entries = StructVector::GetEntries(result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto &target_vector = *child_entries[i];
		if (!state.scan_child_column[i]) {
			// if we are not scanning this vector - set it to NULL
			target_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(target_vector, true);
			continue;
		}
		sub_columns[i]->ScanCommitted(vector_index, state.child_states[i + 1], target_vector, allow_updates,
		                              target_count);
	}
	return scan_count;
}

idx_t StructColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset) {
	auto scan_count = validity.ScanCount(state.child_states[0], result, count);
	auto &child_entries = StructVector::GetEntries(result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto &target_vector = *child_entries[i];
		if (!state.scan_child_column[i]) {
			// if we are not scanning this vector - set it to NULL
			target_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(target_vector, true);
			continue;
		}
		sub_columns[i]->ScanCount(state.child_states[i + 1], target_vector, count, result_offset);
	}
	return scan_count;
}

void StructColumnData::Skip(ColumnScanState &state, idx_t count) {
	validity.Skip(state.child_states[0], count);

	// skip inside the sub-columns
	for (idx_t child_idx = 0; child_idx < sub_columns.size(); child_idx++) {
		if (!state.scan_child_column[child_idx]) {
			continue;
		}
		sub_columns[child_idx]->Skip(state.child_states[child_idx + 1], count);
	}
}

void StructColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnAppendState validity_append;
	validity.InitializeAppend(validity_append);
	state.child_appends.push_back(std::move(validity_append));

	for (auto &sub_column : sub_columns) {
		ColumnAppendState child_append;
		sub_column->InitializeAppend(child_append);
		state.child_appends.push_back(std::move(child_append));
	}
}

void StructColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
		Vector append_vector(vector);
		append_vector.Flatten(count);
		Append(stats, state, append_vector, count);
		return;
	}

	// append the null values
	validity.Append(stats, state.child_appends[0], vector, count);

	auto &child_entries = StructVector::GetEntries(vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->Append(StructStats::GetChildStats(stats, i), state.child_appends[i + 1], *child_entries[i],
		                       count);
	}
	this->count += count;
}

void StructColumnData::RevertAppend(row_t start_row) {
	validity.RevertAppend(start_row);
	for (auto &sub_column : sub_columns) {
		sub_column->RevertAppend(start_row);
	}
	this->count = UnsafeNumericCast<idx_t>(start_row) - this->start;
}

idx_t StructColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	// fetch validity mask
	auto &child_entries = StructVector::GetEntries(result);
	// insert any child states that are required
	for (idx_t i = state.child_states.size(); i < child_entries.size() + 1; i++) {
		ColumnScanState child_state;
		child_state.scan_options = state.scan_options;
		state.child_states.push_back(std::move(child_state));
	}
	// fetch the validity state
	idx_t scan_count = validity.Fetch(state.child_states[0], row_id, result);
	// fetch the sub-column states
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->Fetch(state.child_states[i + 1], row_id, *child_entries[i]);
	}
	return scan_count;
}

void StructColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                              idx_t update_count) {
	validity.Update(transaction, column_index, update_vector, row_ids, update_count);
	auto &child_entries = StructVector::GetEntries(update_vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->Update(transaction, column_index, *child_entries[i], row_ids, update_count);
	}
}

void StructColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path,
                                    Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	// we can never DIRECTLY update a struct column
	if (depth >= column_path.size()) {
		throw InternalException("Attempting to directly update a struct column - this should not be possible");
	}
	auto update_column = column_path[depth];
	if (update_column == 0) {
		// update the validity column
		validity.UpdateColumn(transaction, column_path, update_vector, row_ids, update_count, depth + 1);
	} else {
		if (update_column > sub_columns.size()) {
			throw InternalException("Update column_path out of range");
		}
		sub_columns[update_column - 1]->UpdateColumn(transaction, column_path, update_vector, row_ids, update_count,
		                                             depth + 1);
	}
}

unique_ptr<BaseStatistics> StructColumnData::GetUpdateStatistics() {
	// check if any child column has updates
	auto stats = BaseStatistics::CreateEmpty(type);
	auto validity_stats = validity.GetUpdateStatistics();
	if (validity_stats) {
		stats.Merge(*validity_stats);
	}
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto child_stats = sub_columns[i]->GetUpdateStatistics();
		if (child_stats) {
			StructStats::SetChildStats(stats, i, std::move(child_stats));
		}
	}
	return stats.ToUnique();
}

void StructColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                                idx_t result_idx) {
	// fetch validity mask
	auto &child_entries = StructVector::GetEntries(result);
	// insert any child states that are required
	for (idx_t i = state.child_states.size(); i < child_entries.size() + 1; i++) {
		auto child_state = make_uniq<ColumnFetchState>();
		state.child_states.push_back(std::move(child_state));
	}
	// fetch the validity state
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);
	// fetch the sub-column states
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->FetchRow(transaction, *state.child_states[i + 1], row_id, *child_entries[i], result_idx);
	}
}

void StructColumnData::CommitDropColumn() {
	validity.CommitDropColumn();
	for (auto &sub_column : sub_columns) {
		sub_column->CommitDropColumn();
	}
}

struct StructColumnCheckpointState : public ColumnCheckpointState {
	StructColumnCheckpointState(RowGroup &row_group, ColumnData &column_data,
	                            PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		global_stats = StructStats::CreateEmpty(column_data.type).ToUnique();
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	vector<unique_ptr<ColumnCheckpointState>> child_states;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		D_ASSERT(global_stats);
		for (idx_t i = 0; i < child_states.size(); i++) {
			StructStats::SetChildStats(*global_stats, i, child_states[i]->GetStatistics());
		}
		return std::move(global_stats);
	}

	PersistentColumnData ToPersistentData() override {
		PersistentColumnData data(PhysicalType::STRUCT);
		data.child_columns.push_back(validity_state->ToPersistentData());
		for (auto &child_state : child_states) {
			data.child_columns.push_back(child_state->ToPersistentData());
		}
		return data;
	}
};

unique_ptr<ColumnCheckpointState> StructColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                          PartialBlockManager &partial_block_manager) {
	return make_uniq<StructColumnCheckpointState>(row_group, *this, partial_block_manager);
}

unique_ptr<ColumnCheckpointState> StructColumnData::Checkpoint(RowGroup &row_group,
                                                               ColumnCheckpointInfo &checkpoint_info) {
	auto checkpoint_state = make_uniq<StructColumnCheckpointState>(row_group, *this, checkpoint_info.info.manager);
	checkpoint_state->validity_state = validity.Checkpoint(row_group, checkpoint_info);
	for (auto &sub_column : sub_columns) {
		checkpoint_state->child_states.push_back(sub_column->Checkpoint(row_group, checkpoint_info));
	}
	return std::move(checkpoint_state);
}

bool StructColumnData::IsPersistent() {
	if (!validity.IsPersistent()) {
		return false;
	}
	for (auto &child_col : sub_columns) {
		if (!child_col->IsPersistent()) {
			return false;
		}
	}
	return true;
}

PersistentColumnData StructColumnData::Serialize() {
	PersistentColumnData persistent_data(PhysicalType::STRUCT);
	persistent_data.child_columns.push_back(validity.Serialize());
	for (auto &sub_column : sub_columns) {
		persistent_data.child_columns.push_back(sub_column->Serialize());
	}
	return persistent_data;
}

void StructColumnData::InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) {
	validity.InitializeColumn(column_data.child_columns[0], target_stats);
	for (idx_t c_idx = 0; c_idx < sub_columns.size(); c_idx++) {
		auto &child_stats = StructStats::GetChildStats(target_stats, c_idx);
		sub_columns[c_idx]->InitializeColumn(column_data.child_columns[c_idx + 1], child_stats);
	}
	this->count = validity.count.load();
}

void StructColumnData::GetColumnSegmentInfo(duckdb::idx_t row_group_index, vector<duckdb::idx_t> col_path,
                                            vector<duckdb::ColumnSegmentInfo> &result) {
	col_path.push_back(0);
	validity.GetColumnSegmentInfo(row_group_index, col_path, result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		col_path.back() = i + 1;
		sub_columns[i]->GetColumnSegmentInfo(row_group_index, col_path, result);
	}
}

void StructColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);
	validity.Verify(parent);
	for (auto &sub_column : sub_columns) {
		sub_column->Verify(parent);
	}
#endif
}

} // namespace duckdb
