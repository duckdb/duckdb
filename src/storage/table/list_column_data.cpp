#include "duckdb/storage/table/list_column_data.hpp"
#include "duckdb/storage/statistics/list_statistics.hpp"

namespace duckdb {

ListColumnData::ListColumnData(DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type_p,
                                   ColumnData *parent)
    : ColumnData(info, column_index, start_row, move(type_p), parent), validity(info, 0, start_row, this) {
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	auto &child_type = ListType::GetChildType(type);
	// the child column, with column index 1 (0 is the validity mask)
	child_column = ColumnData::CreateColumnUnique(info, 1, start_row, child_type, this);
}

bool ListColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	// table filters are not supported yet for struct columns
	return false;
}

void ListColumnData::InitializeScan(ColumnScanState &state) {
	ColumnData::InitializeScan(state);

	// initialize the validity segment
	ColumnScanState validity_state;
	validity.InitializeScan(validity_state);
	state.child_states.push_back(move(validity_state));

	// initialize the child scan
	ColumnScanState child_state;
	child_column->InitializeScan(child_state);
	state.child_states.push_back(move(child_state));
}

void ListColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	ColumnData::InitializeScanWithOffset(state, row_idx);

	// initialize the validity segment
	ColumnScanState validity_state;
	validity.InitializeScanWithOffset(validity_state, row_idx);
	state.child_states.push_back(move(validity_state));

	// FIXME: need to read the list at position row_idx and get the row offset of the child
	throw NotImplementedException("List InitializeScanWithOffset");
}

void ListColumnData::Scan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	D_ASSERT(state.row_index == vector_index * STANDARD_VECTOR_SIZE);
	ScanCount(state, result, STANDARD_VECTOR_SIZE);
}

void ListColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	D_ASSERT(state.row_index == vector_index * STANDARD_VECTOR_SIZE);
	ScanCount(state, result, STANDARD_VECTOR_SIZE);
}

void ListColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	if (count == 0) {
		return;
	}
	// updates not supported for lists
	D_ASSERT(!updates);

	idx_t scan_count = ScanVector(state, result, count);
	validity.ScanCount(state.child_states[0], result, count);

	auto data = FlatVector::GetData<list_entry_t>(result);
	auto &first_entry = data[0];
	auto &last_entry = data[scan_count - 1];

#ifdef DEBUG
	for(idx_t i = 1; i < scan_count; i++) {
		D_ASSERT(data[i].offset == data[i - 1].offset + data[i - 1].length);
	}
#endif

	idx_t child_scan_count = last_entry.offset + last_entry.length - first_entry.offset;
	auto child_vector = make_unique<Vector>(ListType::GetChildType(type));
	ListVector::SetEntry(result, move(child_vector));

	if (child_scan_count > 0) {
		auto &child_entry = ListVector::GetEntry(result);
		child_column->ScanCount(state.child_states[1], child_entry, child_scan_count);
	}
	state.child_states[0].NextVector();

	ListVector::SetListSize(result, child_scan_count);
}

void ListColumnData::InitializeAppend(ColumnAppendState &state) {
	// initialize the list offset append
	ColumnData::InitializeAppend(state);

	// initialize the validity append
	ColumnAppendState validity_append_state;
	validity.InitializeAppend(validity_append_state);
	state.child_appends.push_back(move(validity_append_state));

	// initialize the child column append
	ColumnAppendState child_append_state;
	child_column->InitializeAppend(child_append_state);
	state.child_appends.push_back(move(child_append_state));
}

void ListColumnData::Append(BaseStatistics &stats_p, ColumnAppendState &state, Vector &vector, idx_t count) {
	D_ASSERT(count > 0);
	auto &stats = (ListStatistics &) stats_p;

	vector.Normalify(count);
	auto &list_validity = FlatVector::Validity(vector);

	// construct the list_entry_t entries to append to the column data
	auto input_offsets = FlatVector::GetData<list_entry_t>(vector);
	auto start_offset = child_column->GetCount();
	idx_t child_count = 0;

	auto append_offsets = unique_ptr<list_entry_t[]>(new list_entry_t[count]);
	for(idx_t i = 0; i < count; i++) {
		if (list_validity.RowIsValid(i)) {
			append_offsets[i].offset = start_offset + input_offsets[i].offset;
			append_offsets[i].length = input_offsets[i].length;
			child_count += input_offsets[i].length;
		} else {
			if (i > 0) {
				append_offsets[i].offset = append_offsets[i - 1].offset + append_offsets[i - 1].length;
			} else {
				append_offsets[i].offset = start_offset;
			}
			append_offsets[i].length = 0;
		}
	}
#ifdef DEBUG
	D_ASSERT(append_offsets[0].offset == start_offset);
	for(idx_t i = 1; i < count; i++) {
		D_ASSERT(append_offsets[i].offset == append_offsets[i - 1].offset + append_offsets[i - 1].length);
	}
	D_ASSERT(append_offsets[count - 1].offset + append_offsets[count - 1].length - append_offsets[0].offset == child_count);
#endif

	VectorData vdata;
	vdata.validity = list_validity;
	vdata.sel = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	vdata.data = (data_ptr_t) append_offsets.get();

	// append the list offsets
	ColumnData::AppendData(stats, state, vdata, count);
	// append the validity data
	validity.AppendData(*stats.validity_stats, state.child_appends[0], vdata, count);
	// append the child vector
	if (child_count > 0) {
		auto &child_vector = ListVector::GetEntry(vector);
		child_column->Append(*stats.child_stats, state.child_appends[1], child_vector, child_count);
	}
}

void ListColumnData::RevertAppend(row_t start_row) {
	validity.RevertAppend(start_row);
	child_column->RevertAppend(start_row);
}

void ListColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw NotImplementedException("List Fetch");
}

void ListColumnData::Update(Transaction &transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                              idx_t update_count) {
	throw NotImplementedException("List Update is not supported.");
}

void ListColumnData::UpdateColumn(Transaction &transaction, const vector<column_t> &column_path,
                                    Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	throw NotImplementedException("List Update Column is not supported");
}

unique_ptr<BaseStatistics> ListColumnData::GetUpdateStatistics() {
	return nullptr;
}

void ListColumnData::FetchRow(Transaction &transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                                idx_t result_idx) {
	throw NotImplementedException("List FetchRow");
}

void ListColumnData::CommitDropColumn() {
	validity.CommitDropColumn();
	child_column->CommitDropColumn();
}

// struct StructColumnCheckpointState : public ColumnCheckpointState {
// 	StructColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, TableDataWriter &writer)
// 	    : ColumnCheckpointState(row_group, column_data, writer) {
// 		global_stats = make_unique<StructStatistics>(column_data.type);
// 	}

// 	unique_ptr<ColumnCheckpointState> validity_state;
// 	vector<unique_ptr<ColumnCheckpointState>> child_states;

// public:
// 	unique_ptr<BaseStatistics> GetStatistics() override {
// 		auto stats = make_unique<StructStatistics>(column_data.type);
// 		D_ASSERT(stats->child_stats.size() == child_states.size());
// 		stats->validity_stats = validity_state->GetStatistics();
// 		for (idx_t i = 0; i < child_states.size(); i++) {
// 			stats->child_stats[i] = child_states[i]->GetStatistics();
// 			D_ASSERT(stats->child_stats[i]);
// 		}
// 		return move(stats);
// 	}

// 	void FlushToDisk() override {
// 		validity_state->FlushToDisk();
// 		for (auto &state : child_states) {
// 			state->FlushToDisk();
// 		}
// 	}
// };

unique_ptr<ColumnCheckpointState> ListColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                          TableDataWriter &writer) {
	throw NotImplementedException("List CreateCheckpointState");
	// return make_unique<StructColumnCheckpointState>(row_group, *this, writer);
}

unique_ptr<ColumnCheckpointState> ListColumnData::Checkpoint(RowGroup &row_group, TableDataWriter &writer) {
	throw NotImplementedException("List Checkpoint");
}

void ListColumnData::Initialize(PersistentColumnData &column_data) {
	throw NotImplementedException("List Initialize");
}

void ListColumnData::DeserializeColumn(Deserializer &source) {
	throw NotImplementedException("List Deserialize");
}

void ListColumnData::GetStorageInfo(idx_t row_group_index, vector<idx_t> col_path, vector<vector<Value>> &result) {
	col_path.push_back(0);
	validity.GetStorageInfo(row_group_index, col_path, result);
	col_path.back() = 1;
	child_column->GetStorageInfo(row_group_index, col_path, result);
}

} // namespace duckdb
