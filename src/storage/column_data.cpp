#include "storage/column_data.hpp"
#include "storage/table/persistent_segment.hpp"
#include "storage/table/transient_segment.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;


index_t ColumnData::Initialize(vector<unique_ptr<PersistentSegment>>& segments) {
	index_t count = 0;
	for (auto &segment : segments) {
		count += segment->count;
		data.AppendSegment(move(segment));
	}
	return count;
}


void ColumnData::InitializeScan(ColumnScanState &state) {
	state.pointer.segment = (ColumnSegment*) data.GetRootSegment();
	state.pointer.offset = 0;
}


void ColumnData::Scan(Transaction &transaction, Vector &result, ColumnScanState &state, index_t count) {
	// copy data from the column storage
	while (count > 0) {
		// check how much we can copy from this column segment
		index_t to_copy = std::min(count, state.pointer.segment->count - state.pointer.offset);
		if (to_copy > 0) {
			// copy elements from the column segment
			state.pointer.segment->Scan(state.pointer, result, to_copy);
			count -= to_copy;
		}
		if (count > 0) {
			// there is still chunks to copy
			// move to the next segment
			assert(state.pointer.segment->next);
			state.pointer.segment = (ColumnSegment *)state.pointer.segment->next.get();
			state.pointer.offset = 0;
		}
	}
}

void ColumnData::InitializeAppend(ColumnAppendState &state) {
	lock_guard<mutex> tree_lock(data.node_lock);
	state.current = (TransientSegment*) data.GetLastSegment();
	state.row_start = state.current->start + state.current->count;
	assert(state.current->segment_type == ColumnSegmentType::TRANSIENT);
}

void ColumnData::Append(ColumnAppendState &state, Vector &vector) {
	// FIXME: this loop should not be necessary once we rework the transient segments
	index_t offset = 0;
	index_t count = vector.count;
	while(true) {
		// append the data from the vector
		index_t copied_elements = state.current->Append(vector, offset, count);
		if (copied_elements == count) {
			// finished copying everything
			break;
		}

		// we couldn't fit everything we wanted in the current column segment, create a new one
		{
			lock_guard<mutex> tree_lock(data.node_lock);
			AppendTransientSegment(state.current->start + state.current->count);
			state.current = (TransientSegment*) data.GetLastSegment();
		}
		offset += copied_elements;
		count -= copied_elements;
	}
}



void ColumnData::AppendTransientSegment(index_t start_row) {
	auto new_segment = make_unique<TransientSegment>(*table->storage.buffer_manager, type, start_row);
	data.AppendSegment(move(new_segment));
}
