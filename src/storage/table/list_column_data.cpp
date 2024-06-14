#include "duckdb/storage/table/list_column_data.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

ListColumnData::ListColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
                               LogicalType type_p, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, start_row, std::move(type_p), parent),
      validity(block_manager, info, 0, start_row, *this) {
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	auto &child_type = ListType::GetChildType(type);
	// the child column, with column index 1 (0 is the validity mask)
	child_column = ColumnData::CreateColumnUnique(block_manager, info, 1, start_row, child_type, this);
}

void ListColumnData::SetStart(idx_t new_start) {
	ColumnData::SetStart(new_start);
	child_column->SetStart(new_start);
	validity.SetStart(new_start);
}

bool ListColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	// table filters are not supported yet for list columns
	return false;
}

void ListColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) {
	ColumnData::InitializePrefetch(prefetch_state, scan_state, rows);
	validity.InitializePrefetch(prefetch_state, scan_state.child_states[0], rows);

	// we can't know how many rows we need to prefetch for the child of this list without looking at the actual data
	// we make an estimation by looking at how many rows the child column has versus this column
	// e.g if the child column has 10K rows, and we have 1K rows, we estimate that each list has 10 elements
	idx_t rows_per_list = 1;
	if (child_column->count > this->count && this->count > 0) {
		rows_per_list = child_column->count / this->count;
	}
	child_column->InitializePrefetch(prefetch_state, scan_state.child_states[1], rows * rows_per_list);
}

void ListColumnData::InitializeScan(ColumnScanState &state) {
	ColumnData::InitializeScan(state);

	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 2);
	validity.InitializeScan(state.child_states[0]);

	// initialize the child scan
	child_column->InitializeScan(state.child_states[1]);
}

uint64_t ListColumnData::FetchListOffset(idx_t row_idx) {
	auto segment = data.GetSegment(row_idx);
	ColumnFetchState fetch_state;
	Vector result(type, 1);
	segment->FetchRow(fetch_state, UnsafeNumericCast<row_t>(row_idx), result, 0U);

	// initialize the child scan with the required offset
	return FlatVector::GetData<uint64_t>(result)[0];
}

void ListColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	if (row_idx == 0) {
		InitializeScan(state);
		return;
	}
	ColumnData::InitializeScanWithOffset(state, row_idx);

	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 2);
	validity.InitializeScanWithOffset(state.child_states[0], row_idx);

	// we need to read the list at position row_idx to get the correct row offset of the child
	auto child_offset = row_idx == start ? 0 : FetchListOffset(row_idx - 1);
	D_ASSERT(child_offset <= child_column->GetMaxEntry());
	if (child_offset < child_column->GetMaxEntry()) {
		child_column->InitializeScanWithOffset(state.child_states[1], start + child_offset);
	}
	state.last_offset = child_offset;
}

idx_t ListColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                           idx_t scan_count) {
	return ScanCount(state, result, scan_count);
}

idx_t ListColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
                                    idx_t scan_count) {
	return ScanCount(state, result, scan_count);
}

idx_t ListColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	if (count == 0) {
		return 0;
	}
	// updates not supported for lists
	D_ASSERT(!updates);

	Vector offset_vector(LogicalType::UBIGINT, count);
	idx_t scan_count = ScanVector(state, offset_vector, count, ScanVectorType::SCAN_FLAT_VECTOR);
	D_ASSERT(scan_count > 0);
	validity.ScanCount(state.child_states[0], result, count);

	UnifiedVectorFormat offsets;
	offset_vector.ToUnifiedFormat(scan_count, offsets);
	auto data = UnifiedVectorFormat::GetData<uint64_t>(offsets);
	auto last_entry = data[offsets.sel->get_index(scan_count - 1)];

	// shift all offsets so they are 0 at the first entry
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	auto base_offset = state.last_offset;
	idx_t current_offset = 0;
	for (idx_t i = 0; i < scan_count; i++) {
		auto offset_index = offsets.sel->get_index(i);
		result_data[i].offset = current_offset;
		result_data[i].length = data[offset_index] - current_offset - base_offset;
		current_offset += result_data[i].length;
	}

	D_ASSERT(last_entry >= base_offset);
	idx_t child_scan_count = last_entry - base_offset;
	ListVector::Reserve(result, child_scan_count);

	if (child_scan_count > 0) {
		auto &child_entry = ListVector::GetEntry(result);
		if (child_entry.GetType().InternalType() != PhysicalType::STRUCT &&
		    child_entry.GetType().InternalType() != PhysicalType::ARRAY &&
		    state.child_states[1].row_index + child_scan_count > child_column->start + child_column->GetMaxEntry()) {
			throw InternalException("ListColumnData::ScanCount - internal list scan offset is out of range");
		}
		child_column->ScanCount(state.child_states[1], child_entry, child_scan_count);
	}
	state.last_offset = last_entry;

	ListVector::SetListSize(result, child_scan_count);
	return scan_count;
}

void ListColumnData::Skip(ColumnScanState &state, idx_t count) {
	// skip inside the validity segment
	validity.Skip(state.child_states[0], count);

	// we need to read the list entries/offsets to figure out how much to skip
	// note that we only need to read the first and last entry
	// however, let's just read all "count" entries for now
	Vector offset_vector(LogicalType::UBIGINT, count);
	idx_t scan_count = ScanVector(state, offset_vector, count, ScanVectorType::SCAN_FLAT_VECTOR);
	D_ASSERT(scan_count > 0);

	UnifiedVectorFormat offsets;
	offset_vector.ToUnifiedFormat(scan_count, offsets);
	auto data = UnifiedVectorFormat::GetData<uint64_t>(offsets);
	auto last_entry = data[offsets.sel->get_index(scan_count - 1)];
	idx_t child_scan_count = last_entry - state.last_offset;
	if (child_scan_count == 0) {
		return;
	}
	state.last_offset = last_entry;

	// skip the child state forward by the child_scan_count
	child_column->Skip(state.child_states[1], child_scan_count);
}

void ListColumnData::InitializeAppend(ColumnAppendState &state) {
	// initialize the list offset append
	ColumnData::InitializeAppend(state);

	// initialize the validity append
	ColumnAppendState validity_append_state;
	validity.InitializeAppend(validity_append_state);
	state.child_appends.push_back(std::move(validity_append_state));

	// initialize the child column append
	ColumnAppendState child_append_state;
	child_column->InitializeAppend(child_append_state);
	state.child_appends.push_back(std::move(child_append_state));
}

void ListColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	D_ASSERT(count > 0);
	UnifiedVectorFormat list_data;
	vector.ToUnifiedFormat(count, list_data);
	auto &list_validity = list_data.validity;

	// construct the list_entry_t entries to append to the column data
	auto input_offsets = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	auto start_offset = child_column->GetMaxEntry();
	idx_t child_count = 0;

	ValidityMask append_mask(count);
	auto append_offsets = unique_ptr<uint64_t[]>(new uint64_t[count]);
	bool child_contiguous = true;
	for (idx_t i = 0; i < count; i++) {
		auto input_idx = list_data.sel->get_index(i);
		if (list_validity.RowIsValid(input_idx)) {
			auto &input_list = input_offsets[input_idx];
			if (input_list.offset != child_count) {
				child_contiguous = false;
			}
			append_offsets[i] = start_offset + child_count + input_list.length;
			child_count += input_list.length;
		} else {
			append_mask.SetInvalid(i);
			append_offsets[i] = start_offset + child_count;
		}
	}
	auto &list_child = ListVector::GetEntry(vector);
	Vector child_vector(list_child);
	if (!child_contiguous) {
		// if the child of the list vector is a non-contiguous vector (i.e. list elements are repeating or have gaps)
		// we first push a selection vector and flatten the child vector to turn it into a contiguous vector
		SelectionVector child_sel(child_count);
		idx_t current_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto input_idx = list_data.sel->get_index(i);
			if (list_validity.RowIsValid(input_idx)) {
				auto &input_list = input_offsets[input_idx];
				for (idx_t list_idx = 0; list_idx < input_list.length; list_idx++) {
					child_sel.set_index(current_count++, input_list.offset + list_idx);
				}
			}
		}
		D_ASSERT(current_count == child_count);
		child_vector.Slice(list_child, child_sel, child_count);
	}

	UnifiedVectorFormat vdata;
	vdata.sel = FlatVector::IncrementalSelectionVector();
	vdata.data = data_ptr_cast(append_offsets.get());

	// append the list offsets
	ColumnData::AppendData(stats, state, vdata, count);
	// append the validity data
	vdata.validity = append_mask;
	validity.AppendData(stats, state.child_appends[0], vdata, count);
	// append the child vector
	if (child_count > 0) {
		child_column->Append(ListStats::GetChildStats(stats), state.child_appends[1], child_vector, child_count);
	}
}

void ListColumnData::RevertAppend(row_t start_row) {
	ColumnData::RevertAppend(start_row);
	validity.RevertAppend(start_row);
	auto column_count = GetMaxEntry();
	if (column_count > start) {
		// revert append in the child column
		auto list_offset = FetchListOffset(column_count - 1);
		child_column->RevertAppend(UnsafeNumericCast<row_t>(list_offset));
	}
}

idx_t ListColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw NotImplementedException("List Fetch");
}

void ListColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                            idx_t update_count) {
	throw NotImplementedException("List Update is not supported.");
}

void ListColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path,
                                  Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	throw NotImplementedException("List Update Column is not supported");
}

unique_ptr<BaseStatistics> ListColumnData::GetUpdateStatistics() {
	return nullptr;
}

void ListColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                              idx_t result_idx) {
	// insert any child states that are required
	// we need two (validity & list child)
	// note that we need a scan state for the child vector
	// this is because we will (potentially) fetch more than one tuple from the list child
	if (state.child_states.empty()) {
		auto child_state = make_uniq<ColumnFetchState>();
		state.child_states.push_back(std::move(child_state));
	}

	// now perform the fetch within the segment
	auto start_offset = idx_t(row_id) == this->start ? 0 : FetchListOffset(UnsafeNumericCast<idx_t>(row_id - 1));
	auto end_offset = FetchListOffset(UnsafeNumericCast<idx_t>(row_id));
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);

	auto &validity = FlatVector::Validity(result);
	auto list_data = FlatVector::GetData<list_entry_t>(result);
	auto &list_entry = list_data[result_idx];
	// set the list entry offset to the size of the current list
	list_entry.offset = ListVector::GetListSize(result);
	list_entry.length = end_offset - start_offset;
	if (!validity.RowIsValid(result_idx)) {
		// the list is NULL! no need to fetch the child
		D_ASSERT(list_entry.length == 0);
		return;
	}

	// now we need to read from the child all the elements between [offset...length]
	auto child_scan_count = list_entry.length;
	if (child_scan_count > 0) {
		auto child_state = make_uniq<ColumnScanState>();
		auto &child_type = ListType::GetChildType(result.GetType());
		Vector child_scan(child_type, child_scan_count);
		// seek the scan towards the specified position and read [length] entries
		child_state->Initialize(child_type, nullptr);
		child_column->InitializeScanWithOffset(*child_state, start + start_offset);
		D_ASSERT(child_type.InternalType() == PhysicalType::STRUCT ||
		         child_state->row_index + child_scan_count - this->start <= child_column->GetMaxEntry());
		child_column->ScanCount(*child_state, child_scan, child_scan_count);

		ListVector::Append(result, child_scan, child_scan_count);
	}
}

void ListColumnData::CommitDropColumn() {
	ColumnData::CommitDropColumn();
	validity.CommitDropColumn();
	child_column->CommitDropColumn();
}

struct ListColumnCheckpointState : public ColumnCheckpointState {
	ListColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		global_stats = ListStats::CreateEmpty(column_data.type).ToUnique();
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	unique_ptr<ColumnCheckpointState> child_state;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		auto stats = global_stats->Copy();
		ListStats::SetChildStats(stats, child_state->GetStatistics());
		return stats.ToUnique();
	}

	void WriteDataPointers(RowGroupWriter &writer, Serializer &serializer) override {
		ColumnCheckpointState::WriteDataPointers(writer, serializer);
		serializer.WriteObject(101, "validity",
		                       [&](Serializer &serializer) { validity_state->WriteDataPointers(writer, serializer); });
		serializer.WriteObject(102, "child_column",
		                       [&](Serializer &serializer) { child_state->WriteDataPointers(writer, serializer); });
	}
};

unique_ptr<ColumnCheckpointState> ListColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                        PartialBlockManager &partial_block_manager) {
	return make_uniq<ListColumnCheckpointState>(row_group, *this, partial_block_manager);
}

unique_ptr<ColumnCheckpointState> ListColumnData::Checkpoint(RowGroup &row_group,
                                                             ColumnCheckpointInfo &checkpoint_info) {
	auto base_state = ColumnData::Checkpoint(row_group, checkpoint_info);
	auto validity_state = validity.Checkpoint(row_group, checkpoint_info);
	auto child_state = child_column->Checkpoint(row_group, checkpoint_info);

	auto &checkpoint_state = base_state->Cast<ListColumnCheckpointState>();
	checkpoint_state.validity_state = std::move(validity_state);
	checkpoint_state.child_state = std::move(child_state);
	return base_state;
}

void ListColumnData::DeserializeColumn(Deserializer &deserializer, BaseStatistics &target_stats) {
	ColumnData::DeserializeColumn(deserializer, target_stats);

	deserializer.ReadObject(
	    101, "validity", [&](Deserializer &deserializer) { validity.DeserializeColumn(deserializer, target_stats); });

	auto &child_stats = ListStats::GetChildStats(target_stats);
	deserializer.ReadObject(102, "child_column", [&](Deserializer &deserializer) {
		child_column->DeserializeColumn(deserializer, child_stats);
	});
}

void ListColumnData::GetColumnSegmentInfo(duckdb::idx_t row_group_index, vector<duckdb::idx_t> col_path,
                                          vector<duckdb::ColumnSegmentInfo> &result) {
	ColumnData::GetColumnSegmentInfo(row_group_index, col_path, result);
	col_path.push_back(0);
	validity.GetColumnSegmentInfo(row_group_index, col_path, result);
	col_path.back() = 1;
	child_column->GetColumnSegmentInfo(row_group_index, col_path, result);
}

} // namespace duckdb
