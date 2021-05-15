#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/storage/table/transient_segment.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/table/validity_segment.hpp"

#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/storage/string_segment.hpp"
#include "duckdb/storage/table/validity_segment.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/storage/table/row_group.hpp"

namespace duckdb {

ColumnData::ColumnData(DatabaseInstance &db, idx_t start_row, LogicalType type, ColumnData *parent)
    : db(db), start(start_row), type(move(type)), parent(parent) {
}

ColumnData::~ColumnData() {
}

DatabaseInstance &ColumnData::GetDatabase() const {
	return db;
}

const LogicalType &ColumnData::RootType() const {
	if (parent) {
		return parent->RootType();
	}
	return type;
}

void ColumnData::ScanVector(ColumnScanState &state, Vector &result) {
	if (!state.initialized) {
		state.current->InitializeScan(state);
		state.initialized = true;
	}
	idx_t row_index = state.row_index;
	idx_t remaining = STANDARD_VECTOR_SIZE;
	while (remaining > 0) {
		D_ASSERT(row_index >= state.current->start && row_index <= state.current->start + state.current->count);
		idx_t scan_count = MinValue<idx_t>(remaining, state.current->start + state.current->count - row_index);
		idx_t start = row_index - state.current->start;
		idx_t result_offset = STANDARD_VECTOR_SIZE - remaining;
		state.current->Scan(state, start, scan_count, result, result_offset);

		row_index += scan_count;
		remaining -= scan_count;
		if (remaining > 0) {
			if (!state.current->next) {
				break;
			}
			state.current = (ColumnSegment *)state.current->next.get();
			state.current->InitializeScan(state);
			D_ASSERT(row_index >= state.current->start && row_index <= state.current->start + state.current->count);
		}
	}
}

void ColumnData::FilterScan(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count) {
	Scan(state, result);
	result.Slice(sel, approved_tuple_count);
}

void ColumnData::Select(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count,
                        vector<TableFilter> &table_filters) {
	Scan(state, result);
	for (auto &filter : table_filters) {
		UncompressedSegment::FilterSelection(sel, result, filter, approved_tuple_count, FlatVector::Validity(result));
	}
}

void ColumnScanState::Next() {
	//! There is no column segment
	if (!current) {
		return;
	}
	row_index += STANDARD_VECTOR_SIZE;
	while (row_index >= current->start + current->count) {
		current = (ColumnSegment *)current->next.get();
		initialized = false;
		segment_checked = false;
		if (!current) {
			break;
		}
	}
	for (auto &child_state : child_states) {
		child_state.Next();
	}
}

void TableScanState::NextVector() {
	//! nothing to scan for this vector, skip the entire vector
	throw NotImplementedException("FIXME: next vector");
	// for (idx_t j = 0; j < column_ids.size(); j++) {
	// 	column_scans[j].Next();
	// }
}

void ColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	VectorData vdata;
	vector.Orrify(count, vdata);
	AppendData(stats, state, vdata, count);
}

void ColumnData::InitializeAppend(ColumnAppendState &state) {
	lock_guard<mutex> tree_lock(data.node_lock);
	if (data.nodes.empty()) {
		// no segments yet, append an empty segment
		AppendTransientSegment(start);
	}
	auto segment = (ColumnSegment *)data.GetLastSegment();
	if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
		// no transient segments yet
		auto total_rows = segment->start + segment->count;
		AppendTransientSegment(total_rows);
		state.current = (TransientSegment *)data.GetLastSegment();
	} else {
		state.current = (TransientSegment *)segment;
	}

	D_ASSERT(state.current->segment_type == ColumnSegmentType::TRANSIENT);
	state.current->InitializeAppend(state);
}

void ColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, VectorData &vdata, idx_t count) {
	idx_t offset = 0;
	while (true) {
		// append the data from the vector
		idx_t copied_elements = state.current->Append(state, vdata, offset, count);
		stats.Merge(*state.current->stats.statistics);
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

void ColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	// perform the fetch within the segment
	state.row_index = row_id / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE;
	state.current = (ColumnSegment *)data.GetSegment(state.row_index);
	ScanVector(state, result);
}

void ColumnData::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	auto segment = (ColumnSegment *)data.GetSegment(row_id);

	// now perform the fetch within the segment
	segment->FetchRow(state, row_id, result, result_idx);
}

void ColumnData::AppendTransientSegment(idx_t start_row) {
	auto new_segment = make_unique<TransientSegment>(GetDatabase(), type, start_row);
	data.AppendSegment(move(new_segment));
}

void ColumnData::CommitDropColumn() {
	auto &block_manager = BlockManager::GetBlockManager(GetDatabase());
	auto segment = (ColumnSegment *)data.GetRootSegment();
	while (segment) {
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			auto &persistent = (PersistentSegment &)*segment;
			block_manager.MarkBlockAsModified(persistent.block_id);
		}
		segment = (ColumnSegment *)segment->next.get();
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::CreateCheckpointState(RowGroup &row_group, TableDataWriter &writer) {
	return make_unique<ColumnCheckpointState>(row_group, *this, writer);
}

ColumnCheckpointState::ColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, TableDataWriter &writer)
    : row_group(row_group), column_data(column_data), writer(writer) {
}

ColumnCheckpointState::~ColumnCheckpointState() {
}

void ColumnCheckpointState::CreateEmptySegment() {
	auto type_id = column_data.type.InternalType();
	if (type_id == PhysicalType::VARCHAR) {
		auto string_segment = make_unique<StringSegment>(column_data.GetDatabase(), row_group.start);
		string_segment->overflow_writer = make_unique<WriteOverflowStringsToDisk>(column_data.GetDatabase());
		current_segment = move(string_segment);
	} else if (type_id == PhysicalType::BIT) {
		current_segment = make_unique<ValiditySegment>(column_data.GetDatabase(), row_group.start);
	} else {
		current_segment = make_unique<NumericSegment>(column_data.GetDatabase(), type_id, row_group.start);
	}
	segment_stats = make_unique<SegmentStatistics>(column_data.type);
}

void ColumnCheckpointState::AppendData(Vector &data, idx_t count) {
	VectorData vdata;
	data.Orrify(count, vdata);

	idx_t offset = 0;
	while (count > 0) {
		idx_t appended = current_segment->Append(*segment_stats, vdata, offset, count);
		if (appended == count) {
			// appended everything: finished
			return;
		}
		// the segment is full: flush it to disk
		FlushSegment();

		// now create a new segment and continue appending
		CreateEmptySegment();
		offset += appended;
		count -= appended;
	}
}

void ColumnCheckpointState::FlushSegment() {
	auto tuple_count = current_segment->tuple_count.load();
	if (tuple_count == 0) {
		return;
	}

	// get the buffer of the segment and pin it
	auto &buffer_manager = BufferManager::GetBufferManager(column_data.GetDatabase());
	auto &block_manager = BlockManager::GetBlockManager(column_data.GetDatabase());

	auto handle = buffer_manager.Pin(current_segment->block);

	// get a free block id to write to
	auto block_id = block_manager.GetFreeBlockId();

	// construct the data pointer
	uint32_t offset_in_block = 0;

	DataPointer data_pointer;
	data_pointer.block_pointer.block_id = block_id;
	data_pointer.block_pointer.offset = offset_in_block;
	data_pointer.row_start = row_group.start;
	if (!data_pointers.empty()) {
		auto &last_pointer = data_pointers.back();
		data_pointer.row_start = last_pointer.row_start + last_pointer.tuple_count;
	}
	data_pointer.tuple_count = tuple_count;
	data_pointer.statistics = segment_stats->statistics->Copy();

	// construct a persistent segment that points to this block, and append it to the new segment tree
	auto persistent_segment = make_unique<PersistentSegment>(
	    column_data.GetDatabase(), block_id, offset_in_block, column_data.type, data_pointer.row_start,
	    data_pointer.tuple_count, segment_stats->statistics->Copy());
	new_tree.AppendSegment(move(persistent_segment));

	data_pointers.push_back(move(data_pointer));
	// write the block to disk
	block_manager.Write(*handle->node, block_id);

	// merge the segment stats into the global stats
	global_stats->Merge(*segment_stats->statistics);
	handle.reset();

	current_segment.reset();
	segment_stats.reset();
}

void ColumnCheckpointState::FlushToDisk() {
	auto &meta_writer = writer.GetMetaWriter();

	meta_writer.Write<idx_t>(data_pointers.size());
	// then write the data pointers themselves
	for (idx_t k = 0; k < data_pointers.size(); k++) {
		auto &data_pointer = data_pointers[k];
		meta_writer.Write<idx_t>(data_pointer.row_start);
		meta_writer.Write<idx_t>(data_pointer.tuple_count);
		meta_writer.Write<block_id_t>(data_pointer.block_pointer.block_id);
		meta_writer.Write<uint32_t>(data_pointer.block_pointer.offset);
		data_pointer.statistics->Serialize(meta_writer);
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::Checkpoint(RowGroup &row_group, TableDataWriter &writer,
                                                         idx_t column_idx) {
	// scan the segments of the column data
	// set up the checkpoint state
	auto checkpoint_state = CreateCheckpointState(row_group, writer);
	checkpoint_state->global_stats = BaseStatistics::CreateEmpty(type);

	if (!data.root_node) {
		// empty table: flush the empty list
		return checkpoint_state;
	}

	auto &block_manager = BlockManager::GetBlockManager(GetDatabase());
	checkpoint_state->CreateEmptySegment();
	Vector intermediate(type);
	// we create a new segment tree with all the new segments
	// we do this by scanning the current segments of the column and checking for changes
	// if there are any changes (e.g. updates or deletes) we write the new changes
	// otherwise we simply write out the current data pointers
	auto owned_segment = move(data.root_node);
	auto segment = (ColumnSegment *)owned_segment.get();
	while (segment) {
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			auto &persistent = (PersistentSegment &)*segment;
			// persistent segment; check if there were any updates or deletions in this segment
			idx_t start_row_idx = persistent.start - row_group.start;
			idx_t end_row_idx = start_row_idx + persistent.count;
			bool has_changes = false;
			if (!row_group.updates.empty() && column_idx < row_group.updates.size() && row_group.updates[column_idx]) {
				if (row_group.updates[column_idx]->HasUpdates(start_row_idx, end_row_idx)) {
					has_changes = true;
				}
			}
			if (has_changes) {
				// persistent segment has updates: mark it as modified and rewrite the block with the merged updates
				block_manager.MarkBlockAsModified(persistent.block_id);
			} else {
				// unchanged persistent segment: no need to write the data

				// flush any segments preceding this persistent segment
				if (checkpoint_state->current_segment->tuple_count > 0) {
					checkpoint_state->FlushSegment();
					checkpoint_state->CreateEmptySegment();
				}

				// set up the data pointer directly using the data from the persistent segment
				DataPointer pointer;
				pointer.block_pointer.block_id = persistent.block_id;
				pointer.block_pointer.offset = 0;
				pointer.row_start = segment->start;
				pointer.tuple_count = persistent.count;
				pointer.statistics = persistent.stats.statistics->Copy();

				// merge the persistent stats into the global column stats
				checkpoint_state->global_stats->Merge(*persistent.stats.statistics);

				// directly append the current segment to the new tree
				checkpoint_state->new_tree.AppendSegment(move(owned_segment));

				checkpoint_state->data_pointers.push_back(move(pointer));

				// move to the next segment in the list
				owned_segment = move(segment->next);
				segment = (ColumnSegment *)owned_segment.get();
				continue;
			}
		}
		// not persisted yet: scan the segment and write it to disk
		ColumnScanState state;
		state.current = segment;
		segment->InitializeScan(state);

		Vector scan_vector(type);
		for (idx_t base_row_index = 0; base_row_index < segment->count; base_row_index += STANDARD_VECTOR_SIZE) {
			scan_vector.Reference(intermediate);

			idx_t count = MinValue<idx_t>(segment->count - base_row_index, STANDARD_VECTOR_SIZE);
			state.row_index = segment->start + base_row_index;
			segment->Scan(state, base_row_index, count, scan_vector, 0);
			if (!row_group.updates.empty() && column_idx < row_group.updates.size() && row_group.updates[column_idx]) {
				row_group.updates[column_idx]->FetchCommittedRange(segment->start - row_group.start + base_row_index,
				                                                   count, scan_vector);
			}

			checkpoint_state->AppendData(scan_vector, count);
		}
		// move to the next segment in the list
		owned_segment = move(segment->next);
		segment = (ColumnSegment *)owned_segment.get();
	}
	// flush the final segment
	checkpoint_state->FlushSegment();
	// replace the old tree with the new one
	data.Replace(checkpoint_state->new_tree);

	return checkpoint_state;
}

void ColumnData::Initialize(PersistentColumnData &column_data) {
	// load persistent segments
	idx_t segment_rows = 0;
	for (auto &segment : column_data.segments) {
		segment_rows += segment->count;
		data.AppendSegment(move(segment));
	}
	if (segment_rows != column_data.total_rows) {
		throw Exception("Segment rows does not match total rows stored in column...");
	}
}

void ColumnData::BaseDeserialize(DatabaseInstance &db, Deserializer &source, const LogicalType &type,
                                 ColumnData &result) {
	// load the data pointers for the column
	idx_t data_pointer_count = source.Read<idx_t>();
	for (idx_t data_ptr = 0; data_ptr < data_pointer_count; data_ptr++) {
		// read the data pointer
		DataPointer data_pointer;
		data_pointer.row_start = source.Read<idx_t>();
		data_pointer.tuple_count = source.Read<idx_t>();
		data_pointer.block_pointer.block_id = source.Read<block_id_t>();
		data_pointer.block_pointer.offset = source.Read<uint32_t>();
		data_pointer.statistics = BaseStatistics::Deserialize(source, type);

		// create a persistent segment
		auto segment = make_unique<PersistentSegment>(db, data_pointer.block_pointer.block_id,
		                                              data_pointer.block_pointer.offset, type, data_pointer.row_start,
		                                              data_pointer.tuple_count, move(data_pointer.statistics));
		result.data.AppendSegment(move(segment));
	}
}

shared_ptr<ColumnData> ColumnData::Deserialize(DatabaseInstance &db, idx_t start_row, Deserializer &source,
                                               const LogicalType &type) {
	return StandardColumnData::Deserialize(db, start_row, source, type);
}

void ColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	D_ASSERT(this->start == parent.start);
	auto root = data.GetRootSegment();
	if (root) {
		D_ASSERT(root != nullptr);
		D_ASSERT(root->start == this->start);
		idx_t prev_end = root->start;
		while(root) {
			D_ASSERT(prev_end == root->start);
			prev_end = root->start + root->count;
			if (!root->next) {
				D_ASSERT(prev_end == parent.start + parent.count);
			}
			root = root->next.get();
		}
	} else {
		D_ASSERT(parent.count == 0);
	}
#endif
}

} // namespace duckdb
