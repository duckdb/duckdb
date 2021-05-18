#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/storage/table/transient_segment.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

StandardColumnData::StandardColumnData(DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type, ColumnData *parent)
    : ColumnData(info, column_index, start_row, move(type), parent), validity(info, 0, start_row, this) {
}

bool StandardColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	if (!state.segment_checked) {
		state.segment_checked = true;
		if (!state.current) {
			return true;
		}
		if (state.current->stats.CheckZonemap(filter)) {
			return true;
		}
		return false;
		// if (state.updates) {
		// 	return state.updates->GetStatistics().CheckZonemap(filter);
		// } else {
		// 	return false;
		// }
	} else {
		return true;
	}
}

void StandardColumnData::InitializeScan(ColumnScanState &state) {
	// initialize the current segment
	state.current = (ColumnSegment *)data.GetRootSegment();
	state.row_index = state.current ? state.current->start : 0;
	state.initialized = false;

	// initialize the validity segment
	ColumnScanState child_state;
	validity.InitializeScan(child_state);
	state.child_states.push_back(move(child_state));
}

void StandardColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	state.current = (ColumnSegment *)data.GetSegment(row_idx);
	state.row_index = row_idx;
	state.initialized = false;

	// initialize the validity segment
	ColumnScanState child_state;
	validity.InitializeScanWithOffset(child_state, row_idx);
	state.child_states.push_back(move(child_state));
}

void StandardColumnData::Scan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	ColumnData::Scan(transaction, vector_index, state, result);
	validity.Scan(transaction, vector_index, state.child_states[0], result);
	state.Next();
}

void StandardColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	ColumnData::ScanCommitted(vector_index, state, result, allow_updates);
	validity.ScanCommitted(vector_index, state.child_states[0], result, allow_updates);
}

void StandardColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnData::InitializeAppend(state);

	ColumnAppendState child_append;
	validity.InitializeAppend(child_append);
	state.child_appends.push_back(move(child_append));
}

void StandardColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, VectorData &vdata, idx_t count) {
	ColumnData::AppendData(stats, state, vdata, count);

	validity.AppendData(*stats.validity_stats, state.child_appends[0], vdata, count);
}

void StandardColumnData::RevertAppend(row_t start_row) {
	ColumnData::RevertAppend(start_row);

	validity.RevertAppend(start_row);
}

void StandardColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	// fetch validity mask
	if (state.child_states.empty()) {
		ColumnScanState child_state;
		state.child_states.push_back(move(child_state));
	}
	validity.Fetch(state.child_states[0], row_id, result);
	ColumnData::Fetch(state, row_id, result);
}

void StandardColumnData::Update(Transaction &transaction, idx_t column_index, Vector &update_vector, row_t *row_ids, idx_t update_count) {
	ColumnData::Update(transaction, column_index, update_vector, row_ids, update_count);
	validity.Update(transaction, 0, update_vector, row_ids, update_count);
}

unique_ptr<BaseStatistics> StandardColumnData::GetUpdateStatistics() {
	auto stats = updates ? updates->GetStatistics().statistics->Copy() : nullptr;
	auto validity_stats = validity.GetUpdateStatistics();
	if (!stats && !validity_stats) {
		return nullptr;
	}
	if (!stats) {
		stats = make_unique<BaseStatistics>(type);
	}
	stats->validity_stats = move(validity_stats);
	return stats;
}

void StandardColumnData::FetchRow(Transaction &transaction, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	// find the segment the row belongs to
	if (state.child_states.empty()) {
		auto child_state = make_unique<ColumnFetchState>();
		state.child_states.push_back(move(child_state));
	}
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);	ColumnData::FetchRow(transaction, state, row_id, result, result_idx);
}

void StandardColumnData::CommitDropColumn() {
	ColumnData::CommitDropColumn();
	validity.CommitDropColumn();
}

struct StandardColumnCheckpointState : public ColumnCheckpointState {
	StandardColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, TableDataWriter &writer)
	    : ColumnCheckpointState(row_group, column_data, writer) {
	}


	unique_ptr<BaseStatistics> GetStatistics() override {
		auto stats = global_stats->Copy();
		stats->validity_stats = validity_state->GetStatistics();
		return stats;
	}
	unique_ptr<ColumnCheckpointState> validity_state;

	void FlushToDisk() override {
		ColumnCheckpointState::FlushToDisk();
		validity_state->FlushToDisk();
	}
};

unique_ptr<ColumnCheckpointState> StandardColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                            TableDataWriter &writer) {
	return make_unique<StandardColumnCheckpointState>(row_group, *this, writer);
}

unique_ptr<ColumnCheckpointState> StandardColumnData::Checkpoint(RowGroup &row_group, TableDataWriter &writer,
                                                                 idx_t column_idx) {
	auto base_state = ColumnData::Checkpoint(row_group, writer, column_idx);
	auto &checkpoint_state = (StandardColumnCheckpointState &)*base_state;
	checkpoint_state.validity_state = validity.Checkpoint(row_group, writer, column_idx);
	return base_state;
}

void StandardColumnData::Initialize(PersistentColumnData &column_data) {
	auto &persistent = (StandardPersistentColumnData &)column_data;
	ColumnData::Initialize(column_data);
	validity.Initialize(*persistent.validity);
}

shared_ptr<ColumnData> StandardColumnData::Deserialize(DataTableInfo &info, idx_t column_index, idx_t start_row, Deserializer &source,
                                                       const LogicalType &type) {
	auto result = make_shared<StandardColumnData>(info, column_index, start_row, type, nullptr);
	BaseDeserialize(info.db, source, type, *result);
	ColumnData::BaseDeserialize(info.db, source, LogicalType(LogicalTypeId::VALIDITY), result->validity);
	return move(result);
}

} // namespace duckdb
