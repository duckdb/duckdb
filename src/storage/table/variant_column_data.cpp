#include "duckdb/storage/table/variant_column_data.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"

namespace duckdb {

VariantColumnData::VariantColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                     idx_t start_row, LogicalType type_p, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, start_row, std::move(type_p), parent),
      validity(block_manager, info, 0, start_row, *this) {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);

	// the sub column index, starting at 1 (0 is the validity mask)
	idx_t sub_column_index = 1;
	auto unshredded_type = LogicalType::STRUCT(StructType::GetChildTypes(type));
	sub_columns.push_back(
	    ColumnData::CreateColumnUnique(block_manager, info, sub_column_index++, start_row, unshredded_type, this));
	sub_columns.push_back(
	    ColumnData::CreateColumnUnique(block_manager, info, sub_column_index++, start_row, unshredded_type, this));
}

void VariantColumnData::SetStart(idx_t new_start) {
	this->start = new_start;
	for (auto &sub_column : sub_columns) {
		sub_column->SetStart(new_start);
	}
	validity.SetStart(new_start);
}

idx_t VariantColumnData::GetMaxEntry() {
	return sub_columns[0]->GetMaxEntry();
}

void VariantColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) {
	validity.InitializePrefetch(prefetch_state, scan_state.child_states[0], rows);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		if (!scan_state.scan_child_column[i]) {
			continue;
		}
		sub_columns[i]->InitializePrefetch(prefetch_state, scan_state.child_states[i + 1], rows);
	}
}

void VariantColumnData::InitializeScan(ColumnScanState &state) {
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

void VariantColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
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

idx_t VariantColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                              idx_t target_count) {
	auto scan_count = validity.Scan(transaction, vector_index, state.child_states[0], result, target_count);
	sub_columns[0]->Scan(transaction, vector_index, state.child_states[1], result, target_count);
	// auto &child_entries = StructVector::GetEntries(result);
	// for (idx_t i = 0; i < sub_columns.size(); i++) {
	//	auto &target_vector = *child_entries[i];
	//	if (!state.scan_child_column[i]) {
	//		// if we are not scanning this vector - set it to NULL
	//		target_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
	//		ConstantVector::SetNull(target_vector, true);
	//		continue;
	//	}
	//	sub_columns[i]->Scan(transaction, vector_index, state.child_states[i + 1], target_vector, target_count);
	//}
	return scan_count;
}

idx_t VariantColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
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

idx_t VariantColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset) {
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

void VariantColumnData::Skip(ColumnScanState &state, idx_t count) {
	validity.Skip(state.child_states[0], count);

	// skip inside the sub-columns
	for (idx_t child_idx = 0; child_idx < sub_columns.size(); child_idx++) {
		if (!state.scan_child_column[child_idx]) {
			continue;
		}
		sub_columns[child_idx]->Skip(state.child_states[child_idx + 1], count);
	}
}

void VariantColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnAppendState validity_append;
	validity.InitializeAppend(validity_append);
	state.child_appends.push_back(std::move(validity_append));

	for (auto &sub_column : sub_columns) {
		ColumnAppendState child_append;
		sub_column->InitializeAppend(child_append);
		state.child_appends.push_back(std::move(child_append));
	}
}

void VariantColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
		Vector append_vector(vector);
		append_vector.Flatten(count);
		Append(stats, state, append_vector, count);
		return;
	}
	VariantStats::Update(stats, vector, count);

	// append the null values
	validity.Append(stats, state.child_appends[0], vector, count);

	//! FIXME: We could potentially use the min/max stats of the 'type_id' column to skip the iteration in
	//! 'VariantStats' if they are the same, and there are no children (i.e, only primitives)
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->Append(VariantStats::GetUnshreddedStats(stats), state.child_appends[i + 1], vector, count);
	}
	this->count += count;
}

void VariantColumnData::RevertAppend(row_t start_row) {
	validity.RevertAppend(start_row);
	for (auto &sub_column : sub_columns) {
		sub_column->RevertAppend(start_row);
	}
	this->count = UnsafeNumericCast<idx_t>(start_row) - this->start;
}

idx_t VariantColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
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

void VariantColumnData::Update(TransactionData transaction, DataTable &data_table, idx_t column_index,
                               Vector &update_vector, row_t *row_ids, idx_t update_count) {
	validity.Update(transaction, data_table, column_index, update_vector, row_ids, update_count);
	auto &child_entries = StructVector::GetEntries(update_vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->Update(transaction, data_table, column_index, *child_entries[i], row_ids, update_count);
	}
}

void VariantColumnData::UpdateColumn(TransactionData transaction, DataTable &data_table,
                                     const vector<column_t> &column_path, Vector &update_vector, row_t *row_ids,
                                     idx_t update_count, idx_t depth) {
	// we can never DIRECTLY update a struct column
	if (depth >= column_path.size()) {
		throw InternalException("Attempting to directly update a struct column - this should not be possible");
	}
	auto update_column = column_path[depth];
	if (update_column == 0) {
		// update the validity column
		validity.UpdateColumn(transaction, data_table, column_path, update_vector, row_ids, update_count, depth + 1);
	} else {
		if (update_column > sub_columns.size()) {
			throw InternalException("Update column_path out of range");
		}
		sub_columns[update_column - 1]->UpdateColumn(transaction, data_table, column_path, update_vector, row_ids,
		                                             update_count, depth + 1);
	}
}

unique_ptr<BaseStatistics> VariantColumnData::GetUpdateStatistics() {
	// check if any child column has updates
	auto stats = BaseStatistics::CreateEmpty(type);
	auto validity_stats = validity.GetUpdateStatistics();
	if (validity_stats) {
		stats.Merge(*validity_stats);
	}
	auto child_stats = sub_columns[0]->GetUpdateStatistics();
	if (child_stats) {
		VariantStats::SetUnshreddedStats(stats, std::move(child_stats));
	}
	return stats.ToUnique();
}

void VariantColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
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

void VariantColumnData::CommitDropColumn() {
	validity.CommitDropColumn();
	for (auto &sub_column : sub_columns) {
		sub_column->CommitDropColumn();
	}
}

struct VariantColumnCheckpointState : public ColumnCheckpointState {
	VariantColumnCheckpointState(RowGroup &row_group, ColumnData &column_data,
	                             PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		global_stats = VariantStats::CreateEmpty(column_data.type).ToUnique();
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	vector<unique_ptr<ColumnCheckpointState>> child_states;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		D_ASSERT(global_stats);
		VariantStats::SetUnshreddedStats(*global_stats, child_states[0]->GetStatistics());
		return std::move(global_stats);
	}

	PersistentColumnData ToPersistentData() override {
		PersistentColumnData data(column_data.type);
		data.child_columns.push_back(validity_state->ToPersistentData());
		for (auto &child_state : child_states) {
			data.child_columns.push_back(child_state->ToPersistentData());
		}
		return data;
	}
};

unique_ptr<ColumnCheckpointState> VariantColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                           PartialBlockManager &partial_block_manager) {
	return make_uniq<VariantColumnCheckpointState>(row_group, *this, partial_block_manager);
}

unique_ptr<ColumnCheckpointState> VariantColumnData::Checkpoint(RowGroup &row_group,
                                                                ColumnCheckpointInfo &checkpoint_info) {
	auto &partial_block_manager = checkpoint_info.GetPartialBlockManager();
	auto checkpoint_state = make_uniq<VariantColumnCheckpointState>(row_group, *this, partial_block_manager);
	checkpoint_state->validity_state = validity.Checkpoint(row_group, checkpoint_info);
	checkpoint_state->child_states.push_back(sub_columns[0]->Checkpoint(row_group, checkpoint_info));
	checkpoint_state->child_states.push_back(sub_columns[1]->Checkpoint(row_group, checkpoint_info));
	return std::move(checkpoint_state);
}

bool VariantColumnData::IsPersistent() {
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

bool VariantColumnData::HasAnyChanges() const {
	if (validity.HasAnyChanges()) {
		return true;
	}
	for (auto &child_col : sub_columns) {
		if (child_col->HasAnyChanges()) {
			return true;
		}
	}
	return false;
}

PersistentColumnData VariantColumnData::Serialize() {
	PersistentColumnData persistent_data(type);
	persistent_data.child_columns.push_back(validity.Serialize());
	for (auto &sub_column : sub_columns) {
		persistent_data.child_columns.push_back(sub_column->Serialize());
	}
	return persistent_data;
}

void VariantColumnData::InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) {
	validity.InitializeColumn(column_data.child_columns[0], target_stats);
	auto &unshredded_stats = VariantStats::GetUnshreddedStats(target_stats);
	sub_columns[0]->InitializeColumn(column_data.child_columns[1], unshredded_stats);
	sub_columns[1]->InitializeColumn(column_data.child_columns[2], unshredded_stats);
	this->count = validity.count.load();
}

void VariantColumnData::GetColumnSegmentInfo(const QueryContext &context, idx_t row_group_index, vector<idx_t> col_path,
                                             vector<ColumnSegmentInfo> &result) {
	col_path.push_back(0);
	validity.GetColumnSegmentInfo(context, row_group_index, col_path, result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		col_path.back() = i + 1;
		sub_columns[i]->GetColumnSegmentInfo(context, row_group_index, col_path, result);
	}
}

void VariantColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);
	validity.Verify(parent);
	for (auto &sub_column : sub_columns) {
		sub_column->Verify(parent);
	}
#endif
}

} // namespace duckdb
