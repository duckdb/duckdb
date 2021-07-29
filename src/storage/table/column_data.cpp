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
#include "duckdb/storage/table/struct_column_data.hpp"

#include "duckdb/storage/segment/numeric_segment.hpp"
#include "duckdb/storage/segment/string_segment.hpp"
#include "duckdb/storage/table/validity_segment.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/storage/table/list_column_data.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/storage/table/row_group.hpp"

#include "duckdb/function/compression_function.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

ColumnData::ColumnData(DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type, ColumnData *parent)
    : info(info), column_index(column_index), start(start_row), type(move(type)), parent(parent) {
}

ColumnData::~ColumnData() {
}

DatabaseInstance &ColumnData::GetDatabase() const {
	return info.db;
}

DataTableInfo &ColumnData::GetTableInfo() const {
	return info;
}

const LogicalType &ColumnData::RootType() const {
	if (parent) {
		return parent->RootType();
	}
	return type;
}

idx_t ColumnData::GetMaxEntry() {
	auto last_segment = data.GetLastSegment();
	return last_segment ? last_segment->start + last_segment->count : start;
}

void ColumnData::InitializeScan(ColumnScanState &state) {
	state.current = (ColumnSegment *)data.GetRootSegment();
	state.row_index = state.current ? state.current->start : 0;
	state.initialized = false;
}

void ColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	state.current = (ColumnSegment *)data.GetSegment(row_idx);
	state.row_index = row_idx;
	state.initialized = false;
}

idx_t ColumnData::ScanVector(ColumnScanState &state, Vector &result, idx_t remaining) {
	if (!state.initialized) {
		D_ASSERT(state.current);
		state.current->InitializeScan(state);
		state.initialized = true;
	}
	D_ASSERT(state.current->type == type);
	idx_t row_index = state.row_index;
	idx_t initial_remaining = remaining;
	while (remaining > 0) {
		D_ASSERT(row_index >= state.current->start && row_index <= state.current->start + state.current->count);
		idx_t scan_count = MinValue<idx_t>(remaining, state.current->start + state.current->count - row_index);
		idx_t start = row_index - state.current->start;
		idx_t result_offset = initial_remaining - remaining;
		state.current->Scan(state, start, scan_count, result, result_offset, scan_count == initial_remaining);

		row_index += scan_count;
		remaining -= scan_count;
		if (remaining > 0) {
			if (!state.current->next) {
				break;
			}
			state.current = (ColumnSegment *)state.current->next.get();
			state.current->InitializeScan(state);
			state.segment_checked = false;
			D_ASSERT(row_index >= state.current->start && row_index <= state.current->start + state.current->count);
		}
	}
	return initial_remaining - remaining;
}

template <bool SCAN_COMMITTED, bool ALLOW_UPDATES>
idx_t ColumnData::ScanVector(Transaction *transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	auto scan_count = ScanVector(state, result, STANDARD_VECTOR_SIZE);

	lock_guard<mutex> update_guard(update_lock);
	if (updates) {
		if (!ALLOW_UPDATES && updates->HasUncommittedUpdates(vector_index)) {
			throw TransactionException("Cannot create index with outstanding updates");
		}
		result.Normalify(scan_count);
		if (SCAN_COMMITTED) {
			updates->FetchCommitted(vector_index, result);
		} else {
			D_ASSERT(transaction);
			updates->FetchUpdates(*transaction, vector_index, result);
		}
	}
	return scan_count;
}

template idx_t ColumnData::ScanVector<false, false>(Transaction *transaction, idx_t vector_index,
                                                    ColumnScanState &state, Vector &result);
template idx_t ColumnData::ScanVector<true, false>(Transaction *transaction, idx_t vector_index, ColumnScanState &state,
                                                   Vector &result);
template idx_t ColumnData::ScanVector<false, true>(Transaction *transaction, idx_t vector_index, ColumnScanState &state,
                                                   Vector &result);
template idx_t ColumnData::ScanVector<true, true>(Transaction *transaction, idx_t vector_index, ColumnScanState &state,
                                                  Vector &result);

idx_t ColumnData::Scan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	return ScanVector<false, true>(&transaction, vector_index, state, result);
}

idx_t ColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	if (allow_updates) {
		return ScanVector<true, true>(nullptr, vector_index, state, result);
	} else {
		return ScanVector<true, false>(nullptr, vector_index, state, result);
	}
}

void ColumnData::ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t count, Vector &result) {
	ColumnScanState child_state;
	InitializeScanWithOffset(child_state, row_group_start + offset_in_row_group);
	auto scan_count = ScanVector(child_state, result, count);
	if (updates) {
		result.Normalify(scan_count);
		updates->FetchCommittedRange(offset_in_row_group, count, result);
	}
}

idx_t ColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	if (count == 0) {
		return 0;
	}
	// ScanCount can only be used if there are no updates
	D_ASSERT(!updates);
	return ScanVector(state, result, count);
}

void ColumnData::Select(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                        SelectionVector &sel, idx_t &count, const TableFilter &filter) {
	idx_t scan_count = Scan(transaction, vector_index, state, result);
	result.Normalify(scan_count);
	BaseSegment::FilterSelection(sel, result, filter, count, FlatVector::Validity(result));
}

void ColumnData::FilterScan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                            SelectionVector &sel, idx_t count) {
	Scan(transaction, vector_index, state, result);
	result.Slice(sel, count);
}

void ColumnData::FilterScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, SelectionVector &sel,
                                     idx_t count, bool allow_updates) {
	ScanCommitted(vector_index, state, result, allow_updates);
	result.Slice(sel, count);
}

void ColumnData::Skip(ColumnScanState &state, idx_t count) {
	state.Next(count);
}

void ColumnScanState::NextInternal(idx_t count) {
	if (!current) {
		//! There is no column segment
		return;
	}
	row_index += count;
	while (row_index >= current->start + current->count) {
		current = (ColumnSegment *)current->next.get();
		initialized = false;
		segment_checked = false;
		if (!current) {
			break;
		}
	}
	D_ASSERT(!current || (row_index >= current->start && row_index < current->start + current->count));
}

void ColumnScanState::Next(idx_t count) {
	NextInternal(count);
	for (auto &child_state : child_states) {
		child_state.Next(count);
	}
}

void ColumnScanState::NextVector() {
	Next(STANDARD_VECTOR_SIZE);
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

idx_t ColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	D_ASSERT(row_id >= 0);
	D_ASSERT(idx_t(row_id) >= start);
	// perform the fetch within the segment
	state.row_index = start + ((row_id - start) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE);
	state.current = (ColumnSegment *)data.GetSegment(state.row_index);
	return ScanVector(state, result, STANDARD_VECTOR_SIZE);
}

void ColumnData::FetchRow(Transaction &transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                          idx_t result_idx) {
	auto segment = (ColumnSegment *)data.GetSegment(row_id);

	// now perform the fetch within the segment
	segment->FetchRow(state, row_id, result, result_idx);
	// merge any updates made to this row
	lock_guard<mutex> update_guard(update_lock);
	if (updates) {
		updates->FetchRow(transaction, row_id, result, result_idx);
	}
}

void ColumnData::Update(Transaction &transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                        idx_t offset, idx_t update_count) {
	lock_guard<mutex> update_guard(update_lock);
	if (!updates) {
		updates = make_unique<UpdateSegment>(*this);
	}
	Vector base_vector(type);
	ColumnScanState state;
	auto fetch_count = Fetch(state, row_ids[offset], base_vector);

	base_vector.Normalify(fetch_count);
	updates->Update(transaction, column_index, update_vector, row_ids, offset, update_count, base_vector);
}

void ColumnData::UpdateColumn(Transaction &transaction, const vector<column_t> &column_path, Vector &update_vector,
                              row_t *row_ids, idx_t update_count, idx_t depth) {
	// this method should only be called at the end of the path in the base column case
	D_ASSERT(depth >= column_path.size());
	ColumnData::Update(transaction, column_path[0], update_vector, row_ids, 0, update_count);
}

unique_ptr<BaseStatistics> ColumnData::GetUpdateStatistics() {
	lock_guard<mutex> update_guard(update_lock);
	return updates ? updates->GetStatistics() : nullptr;
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
			if (persistent.block_id != INVALID_BLOCK) {
				block_manager.MarkBlockAsModified(persistent.block_id);
			}
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
		D_ASSERT(current_segment->IsUncompressed());
		auto &uncompressed = (UncompressedSegment &) *current_segment;
		idx_t appended = uncompressed.Append(*segment_stats, vdata, offset, count);
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

	bool block_is_constant = segment_stats->statistics->IsConstant();

	block_id_t block_id;
	uint32_t offset_in_block;
	if (!block_is_constant) {
		// get a free block id to write to
		block_id = block_manager.GetFreeBlockId();
		offset_in_block = 0;
	} else {
		block_id = INVALID_BLOCK;
		offset_in_block = 0;
	}

	// construct the data pointer

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

	if (!block_is_constant) {
		// write the block to disk
		auto handle = buffer_manager.Pin(current_segment->block);
		block_manager.Write(*handle->node, block_id);
		handle.reset();
	}

	// merge the segment stats into the global stats
	global_stats->Merge(*segment_stats->statistics);

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

void ColumnData::CheckpointScan(ColumnSegment *segment, ColumnScanState &state, idx_t row_group_start,
                                idx_t base_row_index, idx_t count, Vector &scan_vector) {
	segment->Scan(state, base_row_index, count, scan_vector, 0, true);
	if (updates) {
		scan_vector.Normalify(count);
		updates->FetchCommittedRange(segment->start - row_group_start + base_row_index, count, scan_vector);
	}
}

class ColumnDataCheckpointer {
public:
	ColumnDataCheckpointer(ColumnData &col_data_p, RowGroup &row_group_p, ColumnCheckpointState &state_p) :
		col_data(col_data_p), row_group(row_group_p), state(state_p), is_validity(GetType().id() == LogicalTypeId::VALIDITY),
		intermediate(is_validity ? LogicalType::BOOLEAN : GetType(), true, is_validity) {
		auto &config = DBConfig::GetConfig(GetDatabase());
		compression_functions = config.GetCompressionFunctions(GetType().InternalType());
	}

public:
	DatabaseInstance &GetDatabase() {
		return col_data.GetDatabase();
	}

	const LogicalType &GetType() const {
		return col_data.type;
	}

	void Initialize(unique_ptr<SegmentBase> segment) {
		this->owned_segment = move(segment);
	}

	void ScanSegments(std::function<bool(Vector &, idx_t)> callback) {
		Vector scan_vector(intermediate.GetType(), nullptr);
		for(auto segment = (ColumnSegment *) owned_segment.get(); segment; segment = (ColumnSegment *) segment->next.get()) {
			ColumnScanState scan_state;
			scan_state.current = segment;
			segment->InitializeScan(scan_state);

			for (idx_t base_row_index = 0; base_row_index < segment->count; base_row_index += STANDARD_VECTOR_SIZE) {
				scan_vector.Reference(intermediate);

				idx_t count = MinValue<idx_t>(segment->count - base_row_index, STANDARD_VECTOR_SIZE);
				scan_state.row_index = segment->start + base_row_index;

				col_data.CheckpointScan(segment, scan_state, row_group.start, base_row_index, count, scan_vector);

				if (!callback(scan_vector, count)) {
					return;
				}
			}
		}
	}

	unique_ptr<AnalyzeState> DetectBestCompressionMethod(idx_t &compression_idx) {
		if (compression_functions.empty()) {
			compression_idx = INVALID_INDEX;
			return nullptr;
		}
		// set up the analyze states for each compression method
		vector<unique_ptr<AnalyzeState>> analyze_states;
		analyze_states.reserve(compression_functions.size());
		for(idx_t i = 0; i < compression_functions.size(); i++) {
			analyze_states.push_back(compression_functions[i]->init_analyze(col_data, col_data.type.InternalType()));
		}

		// scan over all the segments and run the analyze step
		ScanSegments([&](Vector &scan_vector, idx_t count) {
			bool has_compression_functions = false;
			for(idx_t i = 0; i < compression_functions.size(); i++) {
				if (!compression_functions[i]) {
					continue;
				}
				auto success = compression_functions[i]->analyze(*analyze_states[i], scan_vector, count);
				if (!success) {
					// could not use this compression function on this data set
					// erase it
					compression_functions[i] = nullptr;
					analyze_states[i].reset();
				} else {
					has_compression_functions = true;
				}
			}
			return has_compression_functions;
		});

		// now that we have passed over all the data, we need to figure out the best method
		// we do this using the final_analyze method
		unique_ptr<AnalyzeState> state;
		compression_idx = INVALID_INDEX;
		idx_t best_score = NumericLimits<idx_t>::Maximum();
		for(idx_t i = 0; i < compression_functions.size(); i++) {
			if (!compression_functions[i]) {
				continue;
			}
			auto score = compression_functions[i]->final_analyze(*analyze_states[i]);
			if (score < best_score) {
				compression_idx = i;
				best_score = score;
				state = move(analyze_states[i]);
			}
		}
		return state;
	}

	void WriteToDisk() {
		// there were changes or transient segments
		// we need to rewrite the column segments to disk

		// first we check the current segments
		// if there are any persistent segments, we will mark their old block ids as modified
		// since the segments will be rewritten their old on disk data is no longer required
		auto &block_manager = BlockManager::GetBlockManager(GetDatabase());
		for(auto segment = (ColumnSegment *) owned_segment.get(); segment; segment = (ColumnSegment *) segment->next.get()) {
			if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
				auto &persistent = (PersistentSegment &)*segment;
				// persistent segment has updates: mark it as modified and rewrite the block with the merged updates
				if (persistent.block_id != INVALID_BLOCK) {
					block_manager.MarkBlockAsModified(persistent.block_id);
				}
			}
		}

		// now we need to write our segment
		// we will first run an analyze step that determines which compression function to use
		idx_t compression_idx;
		auto analyze_state = DetectBestCompressionMethod(compression_idx);

		// now that we have analyzed the compression functions we can start writing to disk
		if (!analyze_state) {
			// no compression method: store uncompressed
			state.CreateEmptySegment();
			ScanSegments([&](Vector &scan_vector, idx_t count) {
				state.AppendData(scan_vector, count);
				return true;
			});
			state.FlushSegment();
		} else {
			throw Exception("FIXME: compression");
		}


		// now we actually write the data to disk
		owned_segment.reset();
	}

	bool HasChanges() {
		for(auto segment = (ColumnSegment *) owned_segment.get(); segment; segment = (ColumnSegment *) segment->next.get()) {
			if (segment->segment_type == ColumnSegmentType::TRANSIENT) {
				// transient segment: always need to write to disk
				return true;
			} else {
				auto &persistent = (PersistentSegment &)*segment;
				// persistent segment; check if there were any updates or deletions in this segment
				idx_t start_row_idx = persistent.start - row_group.start;
				idx_t end_row_idx = start_row_idx + persistent.count;
				if (col_data.updates && col_data.updates->HasUpdates(start_row_idx, end_row_idx)) {
					return true;
				}
			}
		}
		return false;
	}

	void WritePersistentSegments() {
		// all segments are persistent and there are no updates
		// we only need to write the metadata
		auto segment = (ColumnSegment *)owned_segment.get();
		while (segment) {
			auto next_segment = move(segment->next);

			D_ASSERT(segment->segment_type == ColumnSegmentType::PERSISTENT);
			auto &persistent = (PersistentSegment &)*segment;

			// set up the data pointer directly using the data from the persistent segment
			DataPointer pointer;
			pointer.block_pointer.block_id = persistent.block_id;
			pointer.block_pointer.offset = 0;
			pointer.row_start = segment->start;
			pointer.tuple_count = persistent.count;
			pointer.statistics = persistent.stats.statistics->Copy();

			// merge the persistent stats into the global column stats
			state.global_stats->Merge(*persistent.stats.statistics);

			// directly append the current segment to the new tree
			state.new_tree.AppendSegment(move(owned_segment));

			state.data_pointers.push_back(move(pointer));

			// move to the next segment in the list
			owned_segment = move(next_segment);
			segment = (ColumnSegment *)owned_segment.get();
		}
	}


	void Checkpoint() {
		// first check if any of the segments have changes
		if (!HasChanges()) {
			// no changes: only need to write the metadata for this column
			WritePersistentSegments();
		} else {
			// there are changes: rewrite the set of columns
			WriteToDisk();
		}
	}

private:
	ColumnData &col_data;
	RowGroup &row_group;
	ColumnCheckpointState &state;
	bool is_validity;
	Vector intermediate;
	unique_ptr<SegmentBase> owned_segment;
	vector<CompressionFunction*> compression_functions;
};

unique_ptr<ColumnCheckpointState> ColumnData::Checkpoint(RowGroup &row_group, TableDataWriter &writer) {
	// scan the segments of the column data
	// set up the checkpoint state
	auto checkpoint_state = CreateCheckpointState(row_group, writer);
	checkpoint_state->global_stats = BaseStatistics::CreateEmpty(type);

	if (!data.root_node) {
		// empty table: flush the empty list
		return checkpoint_state;
	}
	lock_guard<mutex> update_guard(update_lock);

	ColumnDataCheckpointer checkpointer(*this, row_group, *checkpoint_state);
	checkpointer.Initialize(move(data.root_node));
	checkpointer.Checkpoint();

	// replace the old tree with the new one
	data.Replace(checkpoint_state->new_tree);

	return checkpoint_state;
}

void ColumnData::DeserializeColumn(Deserializer &source) {
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
		auto segment = make_unique<PersistentSegment>(GetDatabase(), data_pointer.block_pointer.block_id,
		                                              data_pointer.block_pointer.offset, type, data_pointer.row_start,
		                                              data_pointer.tuple_count, move(data_pointer.statistics));
		data.AppendSegment(move(segment));
	}
}

shared_ptr<ColumnData> ColumnData::Deserialize(DataTableInfo &info, idx_t column_index, idx_t start_row,
                                               Deserializer &source, const LogicalType &type, ColumnData *parent) {
	auto entry = ColumnData::CreateColumn(info, column_index, start_row, type, parent);
	entry->DeserializeColumn(source);
	return entry;
}

void ColumnData::GetStorageInfo(idx_t row_group_index, vector<idx_t> col_path, vector<vector<Value>> &result) {
	D_ASSERT(!col_path.empty());

	// convert the column path to a string
	string col_path_str = "[";
	for (idx_t i = 0; i < col_path.size(); i++) {
		if (i > 0) {
			col_path_str += ", ";
		}
		col_path_str += to_string(col_path[i]);
	}
	col_path_str += "]";

	// iterate over the segments
	idx_t segment_idx = 0;
	auto segment = (ColumnSegment *)data.GetRootSegment();
	while (segment) {
		vector<Value> column_info;
		// row_group_id
		column_info.push_back(Value::BIGINT(row_group_index));
		// column_id
		column_info.push_back(Value::BIGINT(col_path[0]));
		// column_path
		column_info.emplace_back(col_path_str);
		// segment_id
		column_info.push_back(Value::BIGINT(segment_idx));
		// segment_type
		column_info.emplace_back(type.ToString());
		// start
		column_info.push_back(Value::BIGINT(segment->start));
		// count
		column_info.push_back(Value::BIGINT(segment->count));
		// stats
		column_info.emplace_back(segment->stats.statistics ? segment->stats.statistics->ToString()
		                                                   : string("No Stats"));
		// has_updates
		column_info.push_back(Value::BOOLEAN(updates ? true : false));
		// persistent
		// block_id
		// block_offset
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			auto &persistent = (PersistentSegment &)*segment;
			column_info.push_back(Value::BOOLEAN(true));
			column_info.push_back(Value::BIGINT(persistent.block_id));
			column_info.push_back(Value::BIGINT(persistent.offset));
		} else {
			column_info.push_back(Value::BOOLEAN(false));
			column_info.emplace_back();
			column_info.emplace_back();
		}

		result.push_back(move(column_info));

		segment_idx++;
		segment = (ColumnSegment *)segment->next.get();
	}
}

void ColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	D_ASSERT(this->start == parent.start);
	auto root = data.GetRootSegment();
	if (root) {
		D_ASSERT(root != nullptr);
		D_ASSERT(root->start == this->start);
		idx_t prev_end = root->start;
		while (root) {
			D_ASSERT(prev_end == root->start);
			prev_end = root->start + root->count;
			if (!root->next) {
				D_ASSERT(prev_end == parent.start + parent.count);
			}
			root = root->next.get();
		}
	} else {
		if (type.InternalType() != PhysicalType::STRUCT) {
			D_ASSERT(parent.count == 0);
		}
	}
#endif
}

template <class RET, class OP>
static RET CreateColumnInternal(DataTableInfo &info, idx_t column_index, idx_t start_row, const LogicalType &type,
                                ColumnData *parent) {
	if (type.InternalType() == PhysicalType::STRUCT) {
		return OP::template Create<StructColumnData>(info, column_index, start_row, type, parent);
	} else if (type.InternalType() == PhysicalType::LIST) {
		return OP::template Create<ListColumnData>(info, column_index, start_row, type, parent);
	} else if (type.id() == LogicalTypeId::VALIDITY) {
		return OP::template Create<ValidityColumnData>(info, column_index, start_row, parent);
	}
	return OP::template Create<StandardColumnData>(info, column_index, start_row, type, parent);
}

shared_ptr<ColumnData> ColumnData::CreateColumn(DataTableInfo &info, idx_t column_index, idx_t start_row,
                                                const LogicalType &type, ColumnData *parent) {
	return CreateColumnInternal<shared_ptr<ColumnData>, SharedConstructor>(info, column_index, start_row, type, parent);
}

unique_ptr<ColumnData> ColumnData::CreateColumnUnique(DataTableInfo &info, idx_t column_index, idx_t start_row,
                                                      const LogicalType &type, ColumnData *parent) {
	return CreateColumnInternal<unique_ptr<ColumnData>, UniqueConstructor>(info, column_index, start_row, type, parent);
}

} // namespace duckdb
