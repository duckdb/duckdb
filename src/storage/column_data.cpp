#include "duckdb/storage/column_data.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/storage/table/transient_segment.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/table/update_segment.hpp"

namespace duckdb {

ColumnData::ColumnData(DatabaseInstance &db, DataTableInfo &table_info, LogicalType type, idx_t column_idx)
    : table_info(table_info), type(move(type)), db(db), column_idx(column_idx), persistent_rows(0) {
	statistics = BaseStatistics::CreateEmpty(type);
}

void ColumnData::Initialize(vector<unique_ptr<PersistentSegment>> &segments) {
	for (auto &segment : segments) {
		persistent_rows += segment->count;
		data.AppendSegment(move(segment));
	}
	throw NotImplementedException("FIXME: append empty update segments here");
}

void ColumnData::InitializeScan(ColumnScanState &state) {
	state.current = (ColumnSegment *)data.GetRootSegment();
	state.updates = (UpdateSegment *) updates.GetRootSegment();
	state.vector_index = 0;
	state.vector_index_updates = 0;
	state.initialized = false;
}

void ColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t vector_idx) {
	idx_t row_idx = vector_idx * STANDARD_VECTOR_SIZE;
	state.current = (ColumnSegment *)data.GetSegment(row_idx);
	state.updates = (UpdateSegment *) updates.GetSegment(row_idx);
	state.vector_index = (row_idx - state.current->start) / STANDARD_VECTOR_SIZE;
	state.vector_index_updates = (row_idx - state.updates->start) / STANDARD_VECTOR_SIZE;
	state.initialized = false;
}

void ColumnData::Scan(Transaction &transaction, ColumnScanState &state, Vector &result) {
	if (!state.initialized) {
		state.current->InitializeScan(state);
		state.initialized = true;
	}
	// perform a scan of this segment
	state.current->Scan(state, state.vector_index, result);

	// merge the updates into the result
	state.updates->FetchUpdates(transaction, state.vector_index_updates, result);

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
	state.current->FilterScan(state, result, sel, approved_tuple_count);
	if (state.updates->HasUpdates()) {
		throw NotImplementedException("FIXME: merge updates");
	}
	// move over to the next vector
	state.Next();
}

void ColumnData::Select(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
                        idx_t &approved_tuple_count, vector<TableFilter> &table_filter) {
	if (!state.initialized) {
		state.current->InitializeScan(state);
		state.initialized = true;
	}
	// perform a scan of this segment
	state.current->Select(state, result, sel, approved_tuple_count, table_filter);
	if (state.updates->HasUpdates()) {
		throw NotImplementedException("FIXME: merge updates");
	}
	// move over to the next vector
	state.Next();
}

void ColumnData::IndexScan(ColumnScanState &state, Vector &result) {
	if (!state.initialized) {
		state.current->InitializeScan(state);
		state.initialized = true;
	}
	// // perform a scan of this segment
	state.current->Scan(state, state.vector_index, result);
	if (state.updates->HasUpdates()) {
		throw TransactionException("Cannot create index with outstanding updates");
	}
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
	vector_index_updates++;
	if (vector_index_updates >= MorselInfo::MORSEL_VECTOR_COUNT) {
		updates = (UpdateSegment *) updates->next.get();
		vector_index_updates = 0;
	}
}

void TableScanState::NextVector() {
	//! nothing to scan for this vector, skip the entire vector
	for (idx_t j = 0; j < column_count; j++) {
		column_scans[j].Next();
	}
}

void ColumnData::InitializeAppend(ColumnAppendState &state) {
	lock_guard<mutex> tree_lock(data.node_lock);
	if (data.nodes.empty()) {
		// no transient segments yet, append one
		AppendTransientSegment(persistent_rows);
	}
	if (updates.nodes.empty()) {
		AppendUpdateSegment(0);
	}
	auto segment = (ColumnSegment *)data.GetLastSegment();
	if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
		// cannot append to persistent segment
		// append a new transient segment
		AppendTransientSegment(persistent_rows);
		segment = (ColumnSegment *)data.GetLastSegment();
	}
	state.current = (TransientSegment *)segment;
	D_ASSERT(state.current->segment_type == ColumnSegmentType::TRANSIENT);
	state.current->InitializeAppend(state);
}

void ColumnData::Append(ColumnAppendState &state, Vector &vector, idx_t count) {
	idx_t offset = 0;
	while (true) {
		// append the data from the vector
		idx_t copied_elements = state.current->Append(state, vector, offset, count);
		statistics->Merge(*state.current->stats.statistics);
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
	// check if this row is in the segment tree at all
	if (idx_t(start_row) >= data.nodes.back().row_start + data.nodes.back().node->count) {
		// the start row is equal to the final portion of the column data: nothing was ever appended here
		D_ASSERT(idx_t(start_row) == data.nodes.back().row_start + data.nodes.back().node->count);
		return;
	}
	// find the segment index that the current row belongs to
	idx_t segment_index = data.GetSegmentIndex(start_row);
	auto segment = data.nodes[segment_index].node;
	auto &transient = (TransientSegment &)*segment;
	D_ASSERT(transient.segment_type == ColumnSegmentType::TRANSIENT);

	// remove any segments AFTER this segment: they should be deleted entirely
	if (segment_index < data.nodes.size() - 1) {
		data.nodes.erase(data.nodes.begin() + segment_index + 1, data.nodes.end());
	}
	segment->next = nullptr;
	transient.RevertAppend(start_row);
}

void ColumnData::Update(Transaction &transaction, Vector &update_vector, Vector &row_ids, idx_t count) {
	idx_t first_id = FlatVector::GetValue<row_t>(row_ids, 0);

	// fetch the raw base data for this segment
	Vector base_data(type);
	auto column_segment = (ColumnSegment *)data.GetSegment(first_id);
	auto vector_index = (first_id - column_segment->start) / STANDARD_VECTOR_SIZE;
	// now perform the fetch within the segment
	ColumnScanState state;
	column_segment->Fetch(state, vector_index, base_data);

	// first find the segment that the update belongs to
	auto segment = (UpdateSegment *)updates.GetSegment(first_id);
	// now perform the update within the segment
	segment->Update(transaction, update_vector, FlatVector::GetData<row_t>(row_ids), count, base_data);
	statistics->Merge(*segment->GetStatistics().statistics);
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
	auto segment = (ColumnSegment *)data.GetSegment(row_id);
	// now perform the fetch within the segment
	segment->FetchRow(state, row_id, result, result_idx);
	throw NotImplementedException("FIXME: merge updates in fetch row");
}

void ColumnData::AppendTransientSegment(idx_t start_row) {
	auto new_segment = make_unique<TransientSegment>(db, type, start_row);
	data.AppendSegment(move(new_segment));
}

void ColumnData::AppendUpdateSegment(idx_t start_row) {
	auto new_segment = make_unique<UpdateSegment>(*this, start_row, UpdateSegment::MORSEL_SIZE);
	updates.AppendSegment(move(new_segment));
}

} // namespace duckdb
