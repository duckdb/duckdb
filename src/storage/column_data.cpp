#include "duckdb/storage/column_data.hpp"
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

namespace duckdb {

ColumnData::ColumnData(DatabaseInstance &db, DataTableInfo &table_info, LogicalType type, idx_t column_idx)
    : table_info(table_info), type(move(type)), db(db), column_idx(column_idx), persistent_rows(0) {
	statistics = BaseStatistics::CreateEmpty(type);
}

void ColumnData::FilterScan(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
                            idx_t &approved_tuple_count) {
	Scan(transaction, state, result);
	result.Slice(sel, approved_tuple_count);
}

void ColumnData::Select(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
                        idx_t &approved_tuple_count, vector<TableFilter> &table_filters) {
	Scan(transaction, state, result);
	for (auto &filter : table_filters) {
		UncompressedSegment::FilterSelection(sel, result, filter, approved_tuple_count, FlatVector::Validity(result));
	}
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
		updates = (UpdateSegment *)updates->next.get();
		vector_index_updates = 0;
	}
	for (auto &child_state : child_states) {
		child_state.Next();
	}
}

void TableScanState::NextVector() {
	//! nothing to scan for this vector, skip the entire vector
	for (idx_t j = 0; j < column_count; j++) {
		column_scans[j].Next();
	}
}

void ColumnData::Append(ColumnAppendState &state, Vector &vector, idx_t count) {
	VectorData vdata;
	vector.Orrify(count, vdata);
	AppendData(state, vdata, count);
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
		// cannot append to persistent segment, convert the last segment into a transient segment
		auto transient = make_unique<TransientSegment>((PersistentSegment &)*segment);
		state.current = (TransientSegment *)transient.get();
		data.nodes.back().node = (SegmentBase *)transient.get();
		if (data.root_node.get() == segment) {
			data.root_node = move(transient);
		} else {
			D_ASSERT(data.nodes.size() >= 2);
			data.nodes[data.nodes.size() - 2].node->next = move(transient);
		}
	} else {
		state.current = (TransientSegment *)segment;
	}
	state.updates = (UpdateSegment *)updates.nodes.back().node;

	D_ASSERT(state.current->segment_type == ColumnSegmentType::TRANSIENT);
	state.current->InitializeAppend(state);
}

void ColumnData::AppendData(ColumnAppendState &state, VectorData &vdata, idx_t count) {
	// append to update segments
	idx_t remaining_update_count = count;
	while (remaining_update_count > 0) {
		idx_t to_append_elements =
		    MinValue<idx_t>(remaining_update_count, UpdateSegment::MORSEL_SIZE - state.updates->count);
		state.updates->count += to_append_elements;
		if (state.updates->count == UpdateSegment::MORSEL_SIZE) {
			// have to append a new segment
			AppendUpdateSegment(state.updates->start + state.updates->count);
			state.updates = (UpdateSegment *)updates.nodes.back().node;
		}
		remaining_update_count -= to_append_elements;
	}

	idx_t offset = 0;
	while (true) {
		// append the data from the vector
		idx_t copied_elements = state.current->Append(state, vdata, offset, count);
		MergeStatistics(*state.current->stats.statistics);
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

	// do the same with the update segments
	idx_t update_segment_idx = updates.GetSegmentIndex(start_row);
	auto update_segment = updates.nodes[update_segment_idx].node;
	// remove any segments AFTER this segment
	if (update_segment_idx < updates.nodes.size() - 1) {
		updates.nodes.erase(updates.nodes.begin() + update_segment_idx + 1, updates.nodes.end());
	}
	// truncate this segment
	update_segment->next = nullptr;
	update_segment->count = start_row - update_segment->start;
}

void ColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	// perform the fetch within the segment
	auto segment = (ColumnSegment *)data.GetSegment(row_id);
	auto vector_index = (row_id - segment->start) / STANDARD_VECTOR_SIZE;
	segment->Fetch(state, vector_index, result);

	// merge any updates
	auto update_segment = (UpdateSegment *)updates.GetSegment(row_id);
	auto update_vector_index = (row_id - update_segment->start) / STANDARD_VECTOR_SIZE;
	update_segment->FetchCommitted(update_vector_index, result);
}

void ColumnData::FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
                          idx_t result_idx) {
	auto segment = (ColumnSegment *)data.GetSegment(row_id);
	auto update_segment = (UpdateSegment *)updates.GetSegment(row_id);

	// now perform the fetch within the segment
	segment->FetchRow(state, row_id, result, result_idx);
	// fetch any (potential) updates
	update_segment->FetchRow(transaction, row_id, result, result_idx);
}

void ColumnData::AppendTransientSegment(idx_t start_row) {
	auto new_segment = make_unique<TransientSegment>(db, type, start_row);
	data.AppendSegment(move(new_segment));
}

void ColumnData::AppendUpdateSegment(idx_t start_row, idx_t count) {
	auto new_segment = make_unique<UpdateSegment>(*this, start_row, count);
	updates.AppendSegment(move(new_segment));
}

void ColumnData::SetStatistics(unique_ptr<BaseStatistics> new_stats) {
	lock_guard<mutex> slock(stats_lock);
	this->statistics = move(new_stats);
}

void ColumnData::MergeStatistics(BaseStatistics &other) {
	lock_guard<mutex> slock(stats_lock);
	statistics->Merge(other);
}

unique_ptr<BaseStatistics> ColumnData::GetStatistics() {
	lock_guard<mutex> slock(stats_lock);
	return statistics->Copy();
}

void ColumnData::CommitDropColumn() {
	auto &block_manager = BlockManager::GetBlockManager(db);
	auto segment = (ColumnSegment *)data.GetRootSegment();
	while (segment) {
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			auto &persistent = (PersistentSegment &)*segment;
			block_manager.MarkBlockAsModified(persistent.block_id);
		}
		segment = (ColumnSegment *)segment->next.get();
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::CreateCheckpointState(TableDataWriter &writer) {
	return make_unique<ColumnCheckpointState>(*this, writer);
}

ColumnCheckpointState::ColumnCheckpointState(ColumnData &column_data, TableDataWriter &writer)
    : column_data(column_data), writer(writer) {
}

ColumnCheckpointState::~ColumnCheckpointState() {
}

void ColumnCheckpointState::CreateEmptySegment() {
	auto type_id = column_data.type.InternalType();
	if (type_id == PhysicalType::VARCHAR) {
		auto string_segment = make_unique<StringSegment>(column_data.db, 0);
		string_segment->overflow_writer = make_unique<WriteOverflowStringsToDisk>(column_data.db);
		current_segment = move(string_segment);
	} else if (type_id == PhysicalType::BIT) {
		current_segment = make_unique<ValiditySegment>(column_data.db, 0);
	} else {
		current_segment = make_unique<NumericSegment>(column_data.db, type_id, 0);
	}
	segment_stats = make_unique<SegmentStatistics>(column_data.type, GetTypeIdSize(type_id));
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
	auto &buffer_manager = BufferManager::GetBufferManager(column_data.db);
	auto &block_manager = BlockManager::GetBlockManager(column_data.db);

	auto handle = buffer_manager.Pin(current_segment->block);

	// get a free block id to write to
	auto block_id = block_manager.GetFreeBlockId();

	// construct the data pointer
	uint32_t offset_in_block = 0;

	DataPointer data_pointer;
	data_pointer.block_id = block_id;
	data_pointer.offset = offset_in_block;
	data_pointer.row_start = 0;
	if (!data_pointers.empty()) {
		auto &last_pointer = data_pointers.back();
		data_pointer.row_start = last_pointer.row_start + last_pointer.tuple_count;
	}
	data_pointer.tuple_count = tuple_count;
	data_pointer.statistics = segment_stats->statistics->Copy();

	// construct a persistent segment that points to this block, and append it to the new segment tree
	auto persistent_segment = make_unique<PersistentSegment>(
	    column_data.db, block_id, offset_in_block, column_data.type, data_pointer.row_start, data_pointer.tuple_count,
	    segment_stats->statistics->Copy());
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

	// serialize the global stats of the column
	global_stats->Serialize(meta_writer);

	meta_writer.Write<idx_t>(data_pointers.size());
	// then write the data pointers themselves
	for (idx_t k = 0; k < data_pointers.size(); k++) {
		auto &data_pointer = data_pointers[k];
		meta_writer.Write<idx_t>(data_pointer.row_start);
		meta_writer.Write<idx_t>(data_pointer.tuple_count);
		meta_writer.Write<block_id_t>(data_pointer.block_id);
		meta_writer.Write<uint32_t>(data_pointer.offset);
		data_pointer.statistics->Serialize(meta_writer);
	}
}

void ColumnData::Checkpoint(TableDataWriter &writer) {
	// scan the segments of the column data
	// set up the checkpoint state
	auto checkpoint_state = CreateCheckpointState(writer);
	checkpoint_state->global_stats = BaseStatistics::CreateEmpty(type);

	if (!data.root_node) {
		// empty table: flush the empty list
		checkpoint_state->FlushToDisk();
		return;
	}

	auto &block_manager = BlockManager::GetBlockManager(db);
	checkpoint_state->CreateEmptySegment();
	Vector intermediate(type);
	// we create a new segment tree with all the new segments
	// we do this by scanning the current segments of the column and checking for changes
	// if there are any changes (e.g. updates or appends) we write the new changes
	// otherwise we simply write out the current data pointers
	auto owned_segment = move(data.root_node);
	auto segment = (ColumnSegment *)owned_segment.get();
	auto update_segment = (UpdateSegment *)updates.root_node.get();
	idx_t update_vector_index = 0;
	while (segment) {
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			auto &persistent = (PersistentSegment &)*segment;
			// persistent segment; check if there were any updates in this segment
			idx_t start_vector_index = persistent.start / STANDARD_VECTOR_SIZE;
			idx_t end_vector_index = (persistent.start + persistent.count) / STANDARD_VECTOR_SIZE;
			bool has_updates = update_segment->HasUpdates(start_vector_index, end_vector_index);
			if (has_updates) {
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
				pointer.block_id = persistent.block_id;
				pointer.offset = 0;
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

				// move the update segment forward
				update_vector_index = end_vector_index;
				update_segment = update_segment->FindSegment(end_vector_index);
				continue;
			}
		}
		// not persisted yet: scan the segment and write it to disk
		ColumnScanState state;
		segment->InitializeScan(state);

		Vector scan_vector(type);
		idx_t base_update_index = update_segment->start / STANDARD_VECTOR_SIZE;
		for (idx_t vector_index = 0; vector_index * STANDARD_VECTOR_SIZE < segment->count; vector_index++) {
			scan_vector.Reference(intermediate);

			idx_t count = MinValue<idx_t>(segment->count - vector_index * STANDARD_VECTOR_SIZE, STANDARD_VECTOR_SIZE);

			segment->Scan(state, vector_index, scan_vector);
			update_segment->FetchCommitted(update_vector_index - base_update_index, scan_vector);

			checkpoint_state->AppendData(scan_vector, count);
			update_vector_index++;
			if (update_vector_index - base_update_index >= UpdateSegment::MORSEL_VECTOR_COUNT) {
				base_update_index += UpdateSegment::MORSEL_VECTOR_COUNT;
				update_segment = (UpdateSegment *)update_segment->next.get();
			}
		}
		// move to the next segment in the list
		owned_segment = move(segment->next);
		segment = (ColumnSegment *)owned_segment.get();
	}
	// flush the final segment
	checkpoint_state->FlushSegment();
	// replace the old tree with the new one
	data.Replace(checkpoint_state->new_tree);

	// flush the meta information/data pointers to disk
	checkpoint_state->FlushToDisk();

	// reset all the updates: they have been persisted to disk and included in the new segments
	update_segment = (UpdateSegment *)updates.root_node.get();
	while (update_segment) {
		update_segment->ClearUpdates();
		update_segment = (UpdateSegment *)update_segment->next.get();
	}
}

void ColumnData::Initialize(PersistentColumnData &column_data) {
	// set up statistics
	SetStatistics(move(column_data.stats));

	persistent_rows = column_data.total_rows;
	// load persistent segments
	idx_t segment_rows = 0;
	for (auto &segment : column_data.segments) {
		segment_rows += segment->count;
		data.AppendSegment(move(segment));
	}
	if (segment_rows != persistent_rows) {
		throw Exception("Segment rows does not match total rows stored in column...");
	}

	// set up the (empty) update segments
	idx_t row_count = 0;
	while (row_count < persistent_rows) {
		idx_t next = MinValue<idx_t>(row_count + UpdateSegment::MORSEL_SIZE, persistent_rows);
		AppendUpdateSegment(row_count, next - row_count);
		row_count = next;
	}
	if (row_count % UpdateSegment::MORSEL_SIZE == 0) {
		AppendUpdateSegment(row_count, 0);
	}
}

void ColumnData::BaseDeserialize(DatabaseInstance &db, Deserializer &source, const LogicalType &type,
                                 PersistentColumnData &result) {
	// load the column statistics
	result.stats = BaseStatistics::Deserialize(source, type);
	result.total_rows = 0;

	// load the data pointers for the column
	idx_t data_pointer_count = source.Read<idx_t>();
	for (idx_t data_ptr = 0; data_ptr < data_pointer_count; data_ptr++) {
		// read the data pointer
		DataPointer data_pointer;
		data_pointer.row_start = source.Read<idx_t>();
		data_pointer.tuple_count = source.Read<idx_t>();
		data_pointer.block_id = source.Read<block_id_t>();
		data_pointer.offset = source.Read<uint32_t>();
		data_pointer.statistics = BaseStatistics::Deserialize(source, type);

		result.total_rows += data_pointer.tuple_count;
		// create a persistent segment
		auto segment =
		    make_unique<PersistentSegment>(db, data_pointer.block_id, data_pointer.offset, type, data_pointer.row_start,
		                                   data_pointer.tuple_count, move(data_pointer.statistics));
		result.segments.push_back(move(segment));
	}
}

unique_ptr<PersistentColumnData> ColumnData::Deserialize(DatabaseInstance &db, Deserializer &source,
                                                         const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::VALIDITY:
		return ValidityColumnData::Deserialize(db, source);
	default:
		return StandardColumnData::Deserialize(db, source, type);
	}
}

} // namespace duckdb
