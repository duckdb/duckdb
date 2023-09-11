#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

StandardColumnData::StandardColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                       idx_t start_row, LogicalType type, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, start_row, std::move(type), parent),
      validity(block_manager, info, 0, start_row, *this) {
}

void StandardColumnData::SetStart(idx_t new_start) {
	ColumnData::SetStart(new_start);
	validity.SetStart(new_start);
}

bool StandardColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	if (!state.segment_checked) {
		if (!state.current) {
			return true;
		}
		state.segment_checked = true;
		auto prune_result = filter.CheckStatistics(state.current->stats.statistics);
		if (prune_result != FilterPropagateResult::FILTER_ALWAYS_FALSE) {
			return true;
		}
		if (updates) {
			auto update_stats = updates->GetStatistics();
			prune_result = filter.CheckStatistics(*update_stats);
			return prune_result != FilterPropagateResult::FILTER_ALWAYS_FALSE;
		} else {
			return false;
		}
	} else {
		return true;
	}
}

void StandardColumnData::InitializeScan(ColumnScanState &state) {
	ColumnData::InitializeScan(state);

	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 1);
	validity.InitializeScan(state.child_states[0]);
}

void StandardColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	ColumnData::InitializeScanWithOffset(state, row_idx);

	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 1);
	validity.InitializeScanWithOffset(state.child_states[0], row_idx);
}

idx_t StandardColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state,
                               Vector &result) {
	D_ASSERT(state.row_index == state.child_states[0].row_index);
	auto scan_count = ColumnData::Scan(transaction, vector_index, state, result);
	validity.Scan(transaction, vector_index, state.child_states[0], result);
	return scan_count;
}

idx_t StandardColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result,
                                        bool allow_updates) {
	D_ASSERT(state.row_index == state.child_states[0].row_index);
	auto scan_count = ColumnData::ScanCommitted(vector_index, state, result, allow_updates);
	validity.ScanCommitted(vector_index, state.child_states[0], result, allow_updates);
	return scan_count;
}

idx_t StandardColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	auto scan_count = ColumnData::ScanCount(state, result, count);
	validity.ScanCount(state.child_states[0], result, count);
	return scan_count;
}

void StandardColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnData::InitializeAppend(state);

	ColumnAppendState child_append;
	validity.InitializeAppend(child_append);
	state.child_appends.push_back(std::move(child_append));
}

void StandardColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata,
                                    idx_t count) {
	ColumnData::AppendData(stats, state, vdata, count);
	validity.AppendData(stats, state.child_appends[0], vdata, count);
}

void StandardColumnData::RevertAppend(row_t start_row) {
	ColumnData::RevertAppend(start_row);

	validity.RevertAppend(start_row);
}

idx_t StandardColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	// fetch validity mask
	if (state.child_states.empty()) {
		ColumnScanState child_state;
		state.child_states.push_back(std::move(child_state));
	}
	auto scan_count = ColumnData::Fetch(state, row_id, result);
	validity.Fetch(state.child_states[0], row_id, result);
	return scan_count;
}

void StandardColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                                idx_t update_count) {
	ColumnData::Update(transaction, column_index, update_vector, row_ids, update_count);
	validity.Update(transaction, column_index, update_vector, row_ids, update_count);
}

void StandardColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path,
                                      Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	if (depth >= column_path.size()) {
		// update this column
		ColumnData::Update(transaction, column_path[0], update_vector, row_ids, update_count);
	} else {
		// update the child column (i.e. the validity column)
		validity.UpdateColumn(transaction, column_path, update_vector, row_ids, update_count, depth + 1);
	}
}

unique_ptr<BaseStatistics> StandardColumnData::GetUpdateStatistics() {
	auto stats = updates ? updates->GetStatistics() : nullptr;
	auto validity_stats = validity.GetUpdateStatistics();
	if (!stats && !validity_stats) {
		return nullptr;
	}
	if (!stats) {
		stats = BaseStatistics::CreateEmpty(type).ToUnique();
	}
	if (validity_stats) {
		stats->Merge(*validity_stats);
	}
	return stats;
}

void StandardColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                                  idx_t result_idx) {
	// find the segment the row belongs to
	if (state.child_states.empty()) {
		auto child_state = make_uniq<ColumnFetchState>();
		state.child_states.push_back(std::move(child_state));
	}
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);
	ColumnData::FetchRow(transaction, state, row_id, result, result_idx);
}

void StandardColumnData::CommitDropColumn() {
	ColumnData::CommitDropColumn();
	validity.CommitDropColumn();
}

struct StandardColumnCheckpointState : public ColumnCheckpointState {
	StandardColumnCheckpointState(RowGroup &row_group, ColumnData &column_data,
	                              PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
	}

	unique_ptr<ColumnCheckpointState> validity_state;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		D_ASSERT(global_stats);
		return std::move(global_stats);
	}

	void WriteDataPointers(RowGroupWriter &writer, Serializer &serializer) override {
		ColumnCheckpointState::WriteDataPointers(writer, serializer);
		serializer.WriteObject(101, "validity",
		                       [&](Serializer &serializer) { validity_state->WriteDataPointers(writer, serializer); });
	}
};

unique_ptr<ColumnCheckpointState>
StandardColumnData::CreateCheckpointState(RowGroup &row_group, PartialBlockManager &partial_block_manager) {
	return make_uniq<StandardColumnCheckpointState>(row_group, *this, partial_block_manager);
}

unique_ptr<ColumnCheckpointState> StandardColumnData::Checkpoint(RowGroup &row_group,
                                                                 PartialBlockManager &partial_block_manager,
                                                                 ColumnCheckpointInfo &checkpoint_info) {
	auto validity_state = validity.Checkpoint(row_group, partial_block_manager, checkpoint_info);
	auto base_state = ColumnData::Checkpoint(row_group, partial_block_manager, checkpoint_info);
	auto &checkpoint_state = base_state->Cast<StandardColumnCheckpointState>();
	checkpoint_state.validity_state = std::move(validity_state);
	return base_state;
}

void StandardColumnData::CheckpointScan(ColumnSegment &segment, ColumnScanState &state, idx_t row_group_start,
                                        idx_t count, Vector &scan_vector) {
	ColumnData::CheckpointScan(segment, state, row_group_start, count, scan_vector);

	idx_t offset_in_row_group = state.row_index - row_group_start;
	validity.ScanCommittedRange(row_group_start, offset_in_row_group, count, scan_vector);
}

void StandardColumnData::DeserializeColumn(Deserializer &deserializer) {
	ColumnData::DeserializeColumn(deserializer);
	deserializer.ReadObject(101, "validity",
	                        [&](Deserializer &deserializer) { validity.DeserializeColumn(deserializer); });
}

void StandardColumnData::GetColumnSegmentInfo(duckdb::idx_t row_group_index, vector<duckdb::idx_t> col_path,
                                              vector<duckdb::ColumnSegmentInfo> &result) {
	ColumnData::GetColumnSegmentInfo(row_group_index, col_path, result);
	col_path.push_back(0);
	validity.GetColumnSegmentInfo(row_group_index, std::move(col_path), result);
}

void StandardColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);
	validity.Verify(parent);
#endif
}

} // namespace duckdb
