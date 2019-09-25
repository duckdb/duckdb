#include "storage/column_data.hpp"
#include "storage/table/persistent_segment.hpp"
#include "storage/table/transient_segment.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

ColumnData::ColumnData() : persistent_rows(0) {

}

void ColumnData::Initialize(vector<unique_ptr<PersistentSegment>>& segments) {
	for (auto &segment : segments) {
		persistent_rows += segment->count;
		persistent.AppendSegment(move(segment));
	}
}


void ColumnData::InitializeTransientScan(TransientScanState &state) {
	state.transient = (TransientSegment*) transient.GetRootSegment();
	state.vector_index = 0;
}

void ColumnData::TransientScan(Transaction &transaction, TransientScanState &state, Vector &result) {
	if (state.vector_index == 0) {
		// first vector of this segment: initialize the scan for this segment
		state.transient->InitializeScan(state);
	}
	// perform a scan of this segment
	state.transient->Scan(transaction, state, result);
	// move over to the next vector
	SkipTransientScan(state);
}

void ColumnData::SkipTransientScan(TransientScanState &state) {
	state.vector_index++;
	if (state.vector_index == state.transient->data.max_vector_count) {
		state.transient = (TransientSegment*) state.transient->next.get();
		state.vector_index = 0;
	}
}

void ColumnData::InitializeAppend(ColumnAppendState &state) {
	lock_guard<mutex> tree_lock(transient.node_lock);
	if (transient.nodes.size() == 0) {
		// no transient segments yet, append one
		AppendTransientSegment(persistent_rows);
	}
	state.current = (TransientSegment*) transient.GetLastSegment();
	state.current->InitializeAppend(state.state);
	assert(state.current->segment_type == ColumnSegmentType::TRANSIENT);
}

void ColumnData::Append(ColumnAppendState &state, Vector &vector) {
	index_t offset = 0;
	index_t count = vector.count;
	while(true) {
		// append the data from the vector
		index_t copied_elements = state.current->Append(state.state, vector, offset, count);
		if (copied_elements == count) {
			// finished copying everything
			break;
		}

		// we couldn't fit everything we wanted in the current column segment, create a new one
		{
			lock_guard<mutex> tree_lock(transient.node_lock);
			AppendTransientSegment(state.current->start + state.current->count);
			state.current = (TransientSegment*) transient.GetLastSegment();
			state.current->InitializeAppend(state.state);
		}
		offset += copied_elements;
		count -= copied_elements;
	}
}

void ColumnData::Update(Transaction &transaction, Vector &updates, row_t *ids) {
	// check if the update is in the persistent segments or in the transient segments
	index_t first_id = ids[updates.sel_vector ? updates.sel_vector[0] : 0];

	if (first_id < persistent_rows) {
		// persistent segment update
		throw Exception("FIXME: persistent update");
	} else {
		// transient segment update, first find the segment that it belongs to
		auto segment = (TransientSegment *) transient.GetSegment(first_id);
		// now perform the update within the segment
		segment->Update(transaction, updates, ids);
	}
}

void ColumnData::AppendTransientSegment(index_t start_row) {
	auto new_segment = make_unique<TransientSegment>(*table->storage.buffer_manager, type, start_row);
	transient.AppendSegment(move(new_segment));
}
