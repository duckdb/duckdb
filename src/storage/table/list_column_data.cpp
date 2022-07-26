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
	// table filters are not supported yet for list columns
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

list_entry_t ListColumnData::FetchListEntry(idx_t row_idx) {
	auto segment = (ColumnSegment *)data.GetSegment(row_idx);
	ColumnFetchState fetch_state;
	Vector result(type, 1);
	segment->FetchRow(fetch_state, row_idx, result, 0);

	// initialize the child scan with the required offset
	auto list_data = FlatVector::GetData<list_entry_t>(result);
	return list_data[0];
}

void ListColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	if (row_idx == 0) {
		InitializeScan(state);
		return;
	}
	ColumnData::InitializeScanWithOffset(state, row_idx);

	// initialize the validity segment
	ColumnScanState validity_state;
	validity.InitializeScanWithOffset(validity_state, row_idx);
	state.child_states.push_back(move(validity_state));

	// we need to read the list at position row_idx to get the correct row offset of the child
	auto list_entry = FetchListEntry(row_idx);
	auto child_offset = list_entry.offset;

	D_ASSERT(child_offset <= child_column->GetMaxEntry());
	ColumnScanState child_state;
	if (child_offset < child_column->GetMaxEntry()) {
		child_column->InitializeScanWithOffset(child_state, child_offset);
	}
	state.child_states.push_back(move(child_state));
}

idx_t ListColumnData::Scan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	return ScanCount(state, result, STANDARD_VECTOR_SIZE);
}

idx_t ListColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	return ScanCount(state, result, STANDARD_VECTOR_SIZE);
}

idx_t ListColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	if (count == 0) {
		return 0;
	}
	// updates not supported for lists
	D_ASSERT(!updates);

	idx_t scan_count = ScanVector(state, result, count);
	D_ASSERT(scan_count > 0);
	validity.ScanCount(state.child_states[0], result, count);

	auto data = FlatVector::GetData<list_entry_t>(result);
	auto first_entry = data[0];
	auto last_entry = data[scan_count - 1];

#ifdef DEBUG
	for (idx_t i = 1; i < scan_count; i++) {
		D_ASSERT(data[i].offset == data[i - 1].offset + data[i - 1].length);
	}
#endif
	// shift all offsets so they are 0 at the first entry
	for (idx_t i = 0; i < scan_count; i++) {
		data[i].offset -= first_entry.offset;
	}

	D_ASSERT(last_entry.offset >= first_entry.offset);
	idx_t child_scan_count = last_entry.offset + last_entry.length - first_entry.offset;
	ListVector::Reserve(result, child_scan_count);

	if (child_scan_count > 0) {
		auto &child_entry = ListVector::GetEntry(result);
		D_ASSERT(child_entry.GetType().InternalType() == PhysicalType::STRUCT ||
		         state.child_states[1].row_index + child_scan_count <= child_column->GetMaxEntry());
		child_column->ScanCount(state.child_states[1], child_entry, child_scan_count);
	}

	ListVector::SetListSize(result, child_scan_count);
	return scan_count;
}

void ListColumnData::Skip(ColumnScanState &state, idx_t count) {
	// skip inside the validity segment
	validity.Skip(state.child_states[0], count);

	// we need to read the list entries/offsets to figure out how much to skip
	// note that we only need to read the first and last entry
	// however, let's just read all "count" entries for now
	auto data = unique_ptr<list_entry_t[]>(new list_entry_t[count]);
	Vector result(type, (data_ptr_t)data.get());
	idx_t scan_count = ScanVector(state, result, count);
	if (scan_count == 0) {
		return;
	}

	auto &first_entry = data[0];
	auto &last_entry = data[scan_count - 1];
	idx_t child_scan_count = last_entry.offset + last_entry.length - first_entry.offset;

	// skip the child state forward by the child_scan_count
	child_column->Skip(state.child_states[1], child_scan_count);
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
	auto &stats = (ListStatistics &)stats_p;

	vector.Flatten(count);
	auto &list_validity = FlatVector::Validity(vector);

	// construct the list_entry_t entries to append to the column data
	auto input_offsets = FlatVector::GetData<list_entry_t>(vector);
	auto start_offset = child_column->GetMaxEntry();
	idx_t child_count = 0;

	auto append_offsets = unique_ptr<list_entry_t[]>(new list_entry_t[count]);
	for (idx_t i = 0; i < count; i++) {
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
	for (idx_t i = 1; i < count; i++) {
		D_ASSERT(append_offsets[i].offset == append_offsets[i - 1].offset + append_offsets[i - 1].length);
	}
	D_ASSERT(append_offsets[count - 1].offset + append_offsets[count - 1].length - append_offsets[0].offset ==
	         child_count);
#endif

	UnifiedVectorFormat vdata;
	vdata.validity = list_validity;
	vdata.sel = FlatVector::IncrementalSelectionVector();
	vdata.data = (data_ptr_t)append_offsets.get();

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
	ColumnData::RevertAppend(start_row);
	validity.RevertAppend(start_row);
	auto column_count = GetMaxEntry();
	if (column_count > start) {
		// revert append in the child column
		auto list_entry = FetchListEntry(column_count - 1);
		child_column->RevertAppend(list_entry.offset + list_entry.length);
	}
}

idx_t ListColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw NotImplementedException("List Fetch");
}

void ListColumnData::Update(Transaction &transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                            idx_t update_count) {
	throw NotImplementedException("List Update is not supported.");
}

void ListColumnData::UpdateColumn(Transaction &transaction, const vector<column_t> &column_path, Vector &update_vector,
                                  row_t *row_ids, idx_t update_count, idx_t depth) {
	throw NotImplementedException("List Update Column is not supported");
}

unique_ptr<BaseStatistics> ListColumnData::GetUpdateStatistics() {
	return nullptr;
}

void ListColumnData::FetchRow(Transaction &transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                              idx_t result_idx) {
	// insert any child states that are required
	// we need two (validity & list child)
	// note that we need a scan state for the child vector
	// this is because we will (potentially) fetch more than one tuple from the list child
	if (state.child_states.empty()) {
		auto child_state = make_unique<ColumnFetchState>();
		state.child_states.push_back(move(child_state));
	}
	// fetch the list_entry_t and the validity mask for that list
	auto segment = (ColumnSegment *)data.GetSegment(row_id);

	// now perform the fetch within the segment
	segment->FetchRow(state, row_id, result, result_idx);
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);

	auto &validity = FlatVector::Validity(result);
	auto list_data = FlatVector::GetData<list_entry_t>(result);
	auto &list_entry = list_data[result_idx];
	auto original_offset = list_entry.offset;
	// set the list entry offset to the size of the current list
	list_entry.offset = ListVector::GetListSize(result);
	if (!validity.RowIsValid(result_idx)) {
		// the list is NULL! no need to fetch the child
		D_ASSERT(list_entry.length == 0);
		return;
	}

	// now we need to read from the child all the elements between [offset...length]
	auto child_scan_count = list_entry.length;
	if (child_scan_count > 0) {
		auto child_state = make_unique<ColumnScanState>();
		auto &child_type = ListType::GetChildType(result.GetType());
		Vector child_scan(child_type, child_scan_count);
		// seek the scan towards the specified position and read [length] entries
		child_column->InitializeScanWithOffset(*child_state, original_offset);
		D_ASSERT(child_type.InternalType() == PhysicalType::STRUCT ||
		         child_state->row_index + child_scan_count <= child_column->GetMaxEntry());
		child_column->ScanCount(*child_state, child_scan, child_scan_count);

		ListVector::Append(result, child_scan, child_scan_count);
	}
}

void ListColumnData::CommitDropColumn() {
	validity.CommitDropColumn();
	child_column->CommitDropColumn();
}

struct ListColumnCheckpointState : public ColumnCheckpointState {
	ListColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, TableDataWriter &writer)
	    : ColumnCheckpointState(row_group, column_data, writer) {
		global_stats = make_unique<ListStatistics>(column_data.type);
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	unique_ptr<ColumnCheckpointState> child_state;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		auto stats = global_stats->Copy();
		auto &list_stats = (ListStatistics &)*stats;
		stats->validity_stats = validity_state->GetStatistics();
		list_stats.child_stats = child_state->GetStatistics();
		return stats;
	}

	void FlushToDisk() override {
		ColumnCheckpointState::FlushToDisk();
		validity_state->FlushToDisk();
		child_state->FlushToDisk();
	}
};

unique_ptr<ColumnCheckpointState> ListColumnData::CreateCheckpointState(RowGroup &row_group, TableDataWriter &writer) {
	return make_unique<ListColumnCheckpointState>(row_group, *this, writer);
}

unique_ptr<ColumnCheckpointState> ListColumnData::Checkpoint(RowGroup &row_group, TableDataWriter &writer,
                                                             ColumnCheckpointInfo &checkpoint_info) {
	auto validity_state = validity.Checkpoint(row_group, writer, checkpoint_info);
	auto base_state = ColumnData::Checkpoint(row_group, writer, checkpoint_info);
	auto child_state = child_column->Checkpoint(row_group, writer, checkpoint_info);

	auto &checkpoint_state = (ListColumnCheckpointState &)*base_state;
	checkpoint_state.validity_state = move(validity_state);
	checkpoint_state.child_state = move(child_state);
	return base_state;
}

void ListColumnData::DeserializeColumn(Deserializer &source) {
	ColumnData::DeserializeColumn(source);
	validity.DeserializeColumn(source);
	child_column->DeserializeColumn(source);
}

void ListColumnData::GetStorageInfo(idx_t row_group_index, vector<idx_t> col_path, vector<vector<Value>> &result) {
	col_path.push_back(0);
	validity.GetStorageInfo(row_group_index, col_path, result);
	col_path.back() = 1;
	child_column->GetStorageInfo(row_group_index, col_path, result);
}

} // namespace duckdb
