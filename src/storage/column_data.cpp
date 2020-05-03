#include "duckdb/storage/column_data.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/storage/table/transient_segment.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

ColumnData::ColumnData(BufferManager &manager, DataTableInfo &table_info)
    : table_info(table_info), manager(manager), persistent_rows(0) {
}

void ColumnData::Initialize(vector<unique_ptr<PersistentSegment>> &segments) {
	for (auto &segment : segments) {
		persistent_rows += segment->count;
		data.AppendSegment(move(segment));
	}
}

void ColumnData::InitializeScan(ColumnScanState &state) {
	state.current = (ColumnSegment *)data.GetRootSegment();
	state.vector_index = 0;
	state.initialized = false;
}

void ColumnData::Scan(Transaction &transaction, ColumnScanState &state, Vector &result) {
	if (!state.initialized) {
		state.current->InitializeScan(state);
		state.initialized = true;
	}
	// perform a scan of this segment
	state.current->Scan(transaction, state, state.vector_index, result);
	// move over to the next vector
	state.Next();
}

void ColumnData::FilterScan(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
                            idx_t &approved_tuple_count) {
	if (!state.initialized) {
		state.current->InitializeScan(state);
		state.initialized = true;
	}
	// perform a scan of this segment
	state.current->FilterScan(transaction, state, result, sel, approved_tuple_count);
	// move over to the next vector
	state.Next();
}

void ColumnData::Select(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
                        idx_t &approved_tuple_count, vector<TableFilter> &tableFilter) {
	if (!state.initialized) {
		state.current->InitializeScan(state);
		state.initialized = true;
	}
	// perform a scan of this segment
	state.current->Select(transaction, state, result, sel, approved_tuple_count, tableFilter);
	// move over to the next vector
	state.Next();
}

void ColumnData::IndexScan(ColumnScanState &state, Vector &result) {
	if (state.vector_index == 0) {
		state.current->InitializeScan(state);
	}
	// perform a scan of this segment
	state.current->IndexScan(state, result);
	// move over to the next vector
	state.Next();
}

void ColumnScanState::Next() {
	//! There is no column segment
	if (!current) {
		return;
	}
	vector_index++;
	if (vector_index * STANDARD_VECTOR_SIZE >= current->count) {
		current = (ColumnSegment *)current->next.get();
		vector_index = 0;
		initialized = false;
		segment_checked = false;
	}
}

void TableScanState::NextVector() {
	//! nothing to scan for this vector, skip the entire vector
	for (idx_t j = 0; j < column_ids.size(); j++) {
		auto column = column_ids[j];
		if (column != COLUMN_IDENTIFIER_ROW_ID) {
			column_scans[j].Next();
		}
	}
}

void ColumnData::InitializeAppend(ColumnAppendState &state) {
	lock_guard<mutex> tree_lock(data.node_lock);
	if (data.nodes.size() == 0) {
		// no transient segments yet, append one
		AppendTransientSegment(persistent_rows);
	}
	auto segment = (ColumnSegment *)data.GetLastSegment();
	if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
		// cannot append to persistent segment, add a transient one
		AppendTransientSegment(persistent_rows);
		state.current = (TransientSegment *)data.GetLastSegment();
	} else {
		state.current = (TransientSegment *)segment;
	}
	assert(state.current->segment_type == ColumnSegmentType::TRANSIENT);
	state.current->InitializeAppend(state);
}

void ColumnData::Append(ColumnAppendState &state, Vector &vector, idx_t count) {
	idx_t offset = 0;
	while (true) {
		// append the data from the vector
		idx_t copied_elements = state.current->Append(state, vector, offset, count);
		if (copied_elements == count) {
			// finished copying everything
			break;
		}

		// we couldn't fit everything we wanted in the current column segment, create a new one
		{
			lock_guard<mutex> tree_lock(data.node_lock);
			AppendTransientSegment(state.current->start + state.current->count);
			state.current = (TransientSegment *)data.GetLastSegment();
			state.current->InitializeAppend(state);
		}
		offset += copied_elements;
		count -= copied_elements;
	}
}

void ColumnData::RevertAppend(row_t start_row) {
	lock_guard<mutex> tree_lock(data.node_lock);
	// find the segment index that the current row belongs to
	idx_t segment_index = data.GetSegmentIndex(start_row);
	auto segment = data.nodes[segment_index].node;
	auto &transient = (TransientSegment &)*segment;
	assert(transient.segment_type == ColumnSegmentType::TRANSIENT);

	// remove any segments AFTER this segment: they should be deleted entirely
	if (segment_index < data.nodes.size() - 1) {
		data.nodes.erase(data.nodes.begin() + segment_index + 1, data.nodes.end());
	}
	segment->next = nullptr;
	transient.RevertAppend(start_row);
}

void ColumnData::Update(Transaction &transaction, Vector &updates, Vector &row_ids, idx_t count) {
	// first find the segment that the update belongs to
	idx_t first_id = FlatVector::GetValue<row_t>(row_ids, 0);
	auto segment = (ColumnSegment *)data.GetSegment(first_id);
	// now perform the update within the segment
	segment->Update(*this, transaction, updates, FlatVector::GetData<row_t>(row_ids), count);
}

void ColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	// find the segment that the row belongs to
	auto segment = (ColumnSegment *)data.GetSegment(row_id);
	auto vector_index = (row_id - segment->start) / STANDARD_VECTOR_SIZE;
	// now perform the fetch within the segment
	segment->Fetch(state, vector_index, result);
}

void ColumnData::FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
                          idx_t result_idx) {
	// find the segment the row belongs to
	auto segment = (TransientSegment *)data.GetSegment(row_id);
	// now perform the fetch within the segment
	segment->FetchRow(state, transaction, row_id, result, result_idx);
}

void ColumnData::AppendTransientSegment(idx_t start_row) {
	auto new_segment = make_unique<TransientSegment>(manager, type, start_row);
	data.AppendSegment(move(new_segment));
}
