#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/distinct_statistics.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/list_column_data.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/storage/table/array_column_data.hpp"
#include "duckdb/storage/table/struct_column_data.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/common/serializer/read_stream.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

ColumnData::ColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
                       LogicalType type_p, optional_ptr<ColumnData> parent)
    : start(start_row), count(0), block_manager(block_manager), info(info), column_index(column_index),
      type(std::move(type_p)), allocation_size(0), parent(parent) {
	if (!parent) {
		stats = make_uniq<SegmentStatistics>(type);
	}
}

ColumnData::~ColumnData() {
}

void ColumnData::SetStart(idx_t new_start) {
	this->start = new_start;
	idx_t offset = 0;
	for (auto &segment : data.Segments()) {
		segment.start = start + offset;
		offset += segment.count;
	}
	data.Reinitialize();
}

DatabaseInstance &ColumnData::GetDatabase() const {
	return info.GetDB().GetDatabase();
}

DataTableInfo &ColumnData::GetTableInfo() const {
	return info;
}

StorageManager &ColumnData::GetStorageManager() const {
	return info.GetDB().GetStorageManager();
}

const LogicalType &ColumnData::RootType() const {
	if (parent) {
		return parent->RootType();
	}
	return type;
}

bool ColumnData::HasUpdates() const {
	lock_guard<mutex> update_guard(update_lock);
	return updates.get();
}

bool ColumnData::HasChanges(idx_t start_row, idx_t end_row) const {
	if (!updates) {
		return false;
	}
	if (updates->HasUpdates(start_row, end_row)) {
		return true;
	}
	return false;
}

void ColumnData::ClearUpdates() {
	lock_guard<mutex> update_guard(update_lock);
	updates.reset();
}

idx_t ColumnData::GetMaxEntry() {
	return count;
}

void ColumnData::InitializeScan(ColumnScanState &state) {
	state.current = data.GetRootSegment();
	state.segment_tree = &data;
	state.row_index = state.current ? state.current->start : 0;
	state.internal_index = state.row_index;
	state.initialized = false;
	state.scan_state.reset();
	state.last_offset = 0;
}

void ColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	state.current = data.GetSegment(row_idx);
	state.segment_tree = &data;
	state.row_index = row_idx;
	state.internal_index = state.current->start;
	state.initialized = false;
	state.scan_state.reset();
	state.last_offset = 0;
}

ScanVectorType ColumnData::GetVectorScanType(ColumnScanState &state, idx_t scan_count, Vector &result) {
	if (result.GetVectorType() != VectorType::FLAT_VECTOR) {
		return ScanVectorType::SCAN_ENTIRE_VECTOR;
	}
	if (HasUpdates()) {
		// if we have updates we need to merge in the updates
		// always need to scan flat vectors
		return ScanVectorType::SCAN_FLAT_VECTOR;
	}
	// check if the current segment has enough data remaining
	idx_t remaining_in_segment = state.current->start + state.current->count - state.row_index;
	if (remaining_in_segment < scan_count) {
		// there is not enough data remaining in the current segment so we need to scan across segments
		// we need flat vectors here
		return ScanVectorType::SCAN_FLAT_VECTOR;
	}
	return ScanVectorType::SCAN_ENTIRE_VECTOR;
}

void ColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t remaining) {
	auto current_segment = scan_state.current;
	if (!current_segment) {
		return;
	}
	if (!scan_state.initialized) {
		// need to prefetch for the current segment if we have not yet initialized the scan for this segment
		scan_state.current->InitializePrefetch(prefetch_state, scan_state);
	}
	idx_t row_index = scan_state.row_index;
	while (remaining > 0) {
		idx_t scan_count = MinValue<idx_t>(remaining, current_segment->start + current_segment->count - row_index);
		remaining -= scan_count;
		row_index += scan_count;
		if (remaining > 0) {
			auto next = data.GetNextSegment(current_segment);
			if (!next) {
				break;
			}
			next->InitializePrefetch(prefetch_state, scan_state);
			current_segment = next;
		}
	}
}

void ColumnData::BeginScanVectorInternal(ColumnScanState &state) {
	state.previous_states.clear();
	if (!state.initialized) {
		D_ASSERT(state.current);
		state.current->InitializeScan(state);
		state.internal_index = state.current->start;
		state.initialized = true;
	}
	D_ASSERT(data.HasSegment(state.current));
	D_ASSERT(state.internal_index <= state.row_index);
	if (state.internal_index < state.row_index) {
		state.current->Skip(state);
	}
	D_ASSERT(state.current->type == type);
}

idx_t ColumnData::ScanVector(ColumnScanState &state, Vector &result, idx_t remaining, ScanVectorType scan_type) {
	if (scan_type == ScanVectorType::SCAN_FLAT_VECTOR && result.GetVectorType() != VectorType::FLAT_VECTOR) {
		throw InternalException("ScanVector called with SCAN_FLAT_VECTOR but result is not a flat vector");
	}
	BeginScanVectorInternal(state);
	idx_t initial_remaining = remaining;
	while (remaining > 0) {
		D_ASSERT(state.row_index >= state.current->start &&
		         state.row_index <= state.current->start + state.current->count);
		idx_t scan_count = MinValue<idx_t>(remaining, state.current->start + state.current->count - state.row_index);
		idx_t result_offset = initial_remaining - remaining;
		if (scan_count > 0) {
			if (state.scan_options && state.scan_options->force_fetch_row) {
				for (idx_t i = 0; i < scan_count; i++) {
					ColumnFetchState fetch_state;
					state.current->FetchRow(fetch_state, UnsafeNumericCast<row_t>(state.row_index + i), result,
					                        result_offset + i);
				}
			} else {
				state.current->Scan(state, scan_count, result, result_offset, scan_type);
			}

			state.row_index += scan_count;
			remaining -= scan_count;
		}

		if (remaining > 0) {
			auto next = data.GetNextSegment(state.current);
			if (!next) {
				break;
			}
			state.previous_states.emplace_back(std::move(state.scan_state));
			state.current = next;
			state.current->InitializeScan(state);
			state.segment_checked = false;
			D_ASSERT(state.row_index >= state.current->start &&
			         state.row_index <= state.current->start + state.current->count);
		}
	}
	state.internal_index = state.row_index;
	return initial_remaining - remaining;
}

void ColumnData::SelectVector(ColumnScanState &state, Vector &result, idx_t target_count, const SelectionVector &sel,
                              idx_t sel_count) {
	BeginScanVectorInternal(state);
	if (state.current->start + state.current->count - state.row_index < target_count) {
		throw InternalException("ColumnData::SelectVector should be able to fetch everything from one segment");
	}
	if (state.scan_options && state.scan_options->force_fetch_row) {
		for (idx_t i = 0; i < sel_count; i++) {
			auto source_idx = sel.get_index(i);
			ColumnFetchState fetch_state;
			state.current->FetchRow(fetch_state, UnsafeNumericCast<row_t>(state.row_index + source_idx), result, i);
		}
	} else {
		state.current->Select(state, target_count, result, sel, sel_count);
	}
	state.row_index += target_count;
	state.internal_index = state.row_index;
}

void ColumnData::FilterVector(ColumnScanState &state, Vector &result, idx_t target_count, SelectionVector &sel,
                              idx_t &sel_count, const TableFilter &filter) {
	BeginScanVectorInternal(state);
	if (state.current->start + state.current->count - state.row_index < target_count) {
		throw InternalException("ColumnData::Filter should be able to fetch everything from one segment");
	}
	state.current->Filter(state, target_count, result, sel, sel_count, filter);
	state.row_index += target_count;
	state.internal_index = state.row_index;
}

unique_ptr<BaseStatistics> ColumnData::GetUpdateStatistics() {
	lock_guard<mutex> update_guard(update_lock);
	return updates ? updates->GetStatistics() : nullptr;
}

void ColumnData::FetchUpdates(TransactionData transaction, idx_t vector_index, Vector &result, idx_t scan_count,
                              bool allow_updates, bool scan_committed) {
	lock_guard<mutex> update_guard(update_lock);
	if (!updates) {
		return;
	}
	if (!allow_updates && updates->HasUncommittedUpdates(vector_index)) {
		throw TransactionException("Cannot create index with outstanding updates");
	}
	result.Flatten(scan_count);
	if (scan_committed) {
		updates->FetchCommitted(vector_index, result);
	} else {
		updates->FetchUpdates(transaction, vector_index, result);
	}
}

void ColumnData::FetchUpdateRow(TransactionData transaction, row_t row_id, Vector &result, idx_t result_idx) {
	lock_guard<mutex> update_guard(update_lock);
	if (!updates) {
		return;
	}
	updates->FetchRow(transaction, NumericCast<idx_t>(row_id), result, result_idx);
}

void ColumnData::UpdateInternal(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                                idx_t update_count, Vector &base_vector) {
	lock_guard<mutex> update_guard(update_lock);
	if (!updates) {
		updates = make_uniq<UpdateSegment>(*this);
	}
	updates->Update(transaction, column_index, update_vector, row_ids, update_count, base_vector);
}

idx_t ColumnData::ScanVector(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                             idx_t target_scan, ScanVectorType scan_type, ScanVectorMode mode) {
	auto scan_count = ScanVector(state, result, target_scan, scan_type);
	if (scan_type != ScanVectorType::SCAN_ENTIRE_VECTOR) {
		// if we are scanning an entire vector we cannot have updates
		bool allow_updates = mode != ScanVectorMode::SCAN_COMMITTED_NO_UPDATES;
		bool scan_committed = mode != ScanVectorMode::REGULAR_SCAN;
		FetchUpdates(transaction, vector_index, result, scan_count, allow_updates, scan_committed);
	}
	return scan_count;
}

idx_t ColumnData::ScanVector(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                             idx_t target_scan, ScanVectorMode mode) {
	auto scan_type = GetVectorScanType(state, target_scan, result);
	return ScanVector(transaction, vector_index, state, result, target_scan, scan_type, mode);
}

idx_t ColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	auto target_count = GetVectorCount(vector_index);
	return Scan(transaction, vector_index, state, result, target_count);
}

idx_t ColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	auto target_count = GetVectorCount(vector_index);
	return ScanCommitted(vector_index, state, result, allow_updates, target_count);
}

idx_t ColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                       idx_t scan_count) {
	return ScanVector(transaction, vector_index, state, result, scan_count, ScanVectorMode::REGULAR_SCAN);
}

idx_t ColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
                                idx_t scan_count) {
	auto mode = allow_updates ? ScanVectorMode::SCAN_COMMITTED : ScanVectorMode::SCAN_COMMITTED_NO_UPDATES;
	TransactionData commit_transaction(0, 0);
	return ScanVector(commit_transaction, vector_index, state, result, scan_count, mode);
}

idx_t ColumnData::GetVectorCount(idx_t vector_index) const {
	idx_t current_row = vector_index * STANDARD_VECTOR_SIZE;
	return MinValue<idx_t>(STANDARD_VECTOR_SIZE, count - current_row);
}

void ColumnData::ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t s_count, Vector &result) {
	ColumnScanState child_state;
	InitializeScanWithOffset(child_state, row_group_start + offset_in_row_group);
	bool has_updates = HasUpdates();
	auto scan_count = ScanVector(child_state, result, s_count, ScanVectorType::SCAN_FLAT_VECTOR);
	if (has_updates) {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		result.Flatten(scan_count);
		updates->FetchCommittedRange(offset_in_row_group, s_count, result);
	}
}

idx_t ColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t scan_count) {
	if (scan_count == 0) {
		return 0;
	}
	// ScanCount can only be used if there are no updates
	D_ASSERT(!HasUpdates());
	return ScanVector(state, result, scan_count, ScanVectorType::SCAN_FLAT_VECTOR);
}

void ColumnData::Filter(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                        SelectionVector &sel, idx_t &s_count, const TableFilter &filter) {
	idx_t scan_count = Scan(transaction, vector_index, state, result);

	UnifiedVectorFormat vdata;
	result.ToUnifiedFormat(scan_count, vdata);
	ColumnSegment::FilterSelection(sel, result, vdata, filter, scan_count, s_count);
}

void ColumnData::Select(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                        SelectionVector &sel, idx_t s_count) {
	Scan(transaction, vector_index, state, result);
	result.Slice(sel, s_count);
}

void ColumnData::SelectCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, SelectionVector &sel,
                                 idx_t s_count, bool allow_updates) {
	ScanCommitted(vector_index, state, result, allow_updates);
	result.Slice(sel, s_count);
}

void ColumnData::Skip(ColumnScanState &state, idx_t s_count) {
	state.Next(s_count);
}

void ColumnData::Append(BaseStatistics &append_stats, ColumnAppendState &state, Vector &vector, idx_t append_count) {
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(append_count, vdata);
	AppendData(append_stats, state, vdata, append_count);
}

void ColumnData::Append(ColumnAppendState &state, Vector &vector, idx_t append_count) {
	if (parent || !stats) {
		throw InternalException("ColumnData::Append called on a column with a parent or without stats");
	}
	lock_guard<mutex> l(stats_lock);
	Append(stats->statistics, state, vector, append_count);
}

FilterPropagateResult ColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	if (state.segment_checked) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (!state.current) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	state.segment_checked = true;
	FilterPropagateResult prune_result;
	{
		lock_guard<mutex> l(stats_lock);
		prune_result = filter.CheckStatistics(state.current->stats.statistics);
		if (prune_result == FilterPropagateResult::NO_PRUNING_POSSIBLE) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
	}
	lock_guard<mutex> l(update_lock);
	if (!updates) {
		// no updates - return original result
		return prune_result;
	}
	auto update_stats = updates->GetStatistics();
	// combine the update and original prune result
	FilterPropagateResult update_result = filter.CheckStatistics(*update_stats);
	if (prune_result == update_result) {
		return prune_result;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

FilterPropagateResult ColumnData::CheckZonemap(TableFilter &filter) {
	if (!stats) {
		throw InternalException("ColumnData::CheckZonemap called on a column without stats");
	}
	lock_guard<mutex> l(stats_lock);
	return filter.CheckStatistics(stats->statistics);
}

unique_ptr<BaseStatistics> ColumnData::GetStatistics() {
	if (!stats) {
		throw InternalException("ColumnData::GetStatistics called on a column without stats");
	}
	lock_guard<mutex> l(stats_lock);
	return stats->statistics.ToUnique();
}

void ColumnData::MergeStatistics(const BaseStatistics &other) {
	if (!stats) {
		throw InternalException("ColumnData::MergeStatistics called on a column without stats");
	}
	lock_guard<mutex> l(stats_lock);
	return stats->statistics.Merge(other);
}

void ColumnData::MergeIntoStatistics(BaseStatistics &other) {
	if (!stats) {
		throw InternalException("ColumnData::MergeIntoStatistics called on a column without stats");
	}
	lock_guard<mutex> l(stats_lock);
	return other.Merge(stats->statistics);
}

void ColumnData::InitializeAppend(ColumnAppendState &state) {
	auto l = data.Lock();
	if (data.IsEmpty(l)) {
		// no segments yet, append an empty segment
		AppendTransientSegment(l, start);
	}
	auto segment = data.GetLastSegment(l);
	if (segment->segment_type == ColumnSegmentType::PERSISTENT || !segment->GetCompressionFunction().init_append) {
		// we cannot append to this segment - append a new segment
		auto total_rows = segment->start + segment->count;
		AppendTransientSegment(l, total_rows);
		state.current = data.GetLastSegment(l);
	} else {
		state.current = segment;
	}

	D_ASSERT(state.current->segment_type == ColumnSegmentType::TRANSIENT);
	state.current->InitializeAppend(state);
	D_ASSERT(state.current->GetCompressionFunction().append);
}

void ColumnData::AppendData(BaseStatistics &append_stats, ColumnAppendState &state, UnifiedVectorFormat &vdata,
                            idx_t append_count) {
	idx_t offset = 0;
	this->count += append_count;
	while (true) {
		// append the data from the vector
		idx_t copied_elements = state.current->Append(state, vdata, offset, append_count);
		append_stats.Merge(state.current->stats.statistics);
		if (copied_elements == append_count) {
			// finished copying everything
			break;
		}

		// we couldn't fit everything we wanted in the current column segment, create a new one
		{
			auto l = data.Lock();
			AppendTransientSegment(l, state.current->start + state.current->count);
			state.current = data.GetLastSegment(l);
			state.current->InitializeAppend(state);
		}
		offset += copied_elements;
		append_count -= copied_elements;
	}
}

void ColumnData::RevertAppend(row_t start_row) {
	auto l = data.Lock();
	// check if this row is in the segment tree at all
	auto last_segment = data.GetLastSegment(l);
	if (NumericCast<idx_t>(start_row) >= last_segment->start + last_segment->count) {
		// the start row is equal to the final portion of the column data: nothing was ever appended here
		D_ASSERT(NumericCast<idx_t>(start_row) == last_segment->start + last_segment->count);
		return;
	}
	// find the segment index that the current row belongs to
	idx_t segment_index = data.GetSegmentIndex(l, UnsafeNumericCast<idx_t>(start_row));
	auto segment = data.GetSegmentByIndex(l, UnsafeNumericCast<int64_t>(segment_index));
	auto &transient = *segment;
	D_ASSERT(transient.segment_type == ColumnSegmentType::TRANSIENT);

	// remove any segments AFTER this segment: they should be deleted entirely
	data.EraseSegments(l, segment_index);

	this->count = UnsafeNumericCast<idx_t>(start_row) - this->start;
	segment->next = nullptr;
	transient.RevertAppend(UnsafeNumericCast<idx_t>(start_row));
}

idx_t ColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	D_ASSERT(row_id >= 0);
	D_ASSERT(NumericCast<idx_t>(row_id) >= start);
	// perform the fetch within the segment
	state.row_index =
	    start + ((UnsafeNumericCast<idx_t>(row_id) - start) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE);
	state.current = data.GetSegment(state.row_index);
	state.internal_index = state.current->start;
	return ScanVector(state, result, STANDARD_VECTOR_SIZE, ScanVectorType::SCAN_FLAT_VECTOR);
}

void ColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                          idx_t result_idx) {
	auto segment = data.GetSegment(UnsafeNumericCast<idx_t>(row_id));

	// now perform the fetch within the segment
	segment->FetchRow(state, row_id, result, result_idx);
	// merge any updates made to this row

	FetchUpdateRow(transaction, row_id, result, result_idx);
}

void ColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                        idx_t update_count) {
	Vector base_vector(type);
	ColumnScanState state;
	auto fetch_count = Fetch(state, row_ids[0], base_vector);

	base_vector.Flatten(fetch_count);
	UpdateInternal(transaction, column_index, update_vector, row_ids, update_count, base_vector);
}

void ColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path, Vector &update_vector,
                              row_t *row_ids, idx_t update_count, idx_t depth) {
	// this method should only be called at the end of the path in the base column case
	D_ASSERT(depth >= column_path.size());
	ColumnData::Update(transaction, column_path[0], update_vector, row_ids, update_count);
}

void ColumnData::AppendTransientSegment(SegmentLock &l, idx_t start_row) {

	const auto block_size = block_manager.GetBlockSize();
	const auto type_size = GetTypeIdSize(type.InternalType());
	auto vector_segment_size = block_size;

	if (start_row == NumericCast<idx_t>(MAX_ROW_ID)) {
#if STANDARD_VECTOR_SIZE < 1024
		vector_segment_size = 1024 * type_size;
#else
		vector_segment_size = STANDARD_VECTOR_SIZE * type_size;
#endif
	}

	// The segment size is bound by the block size, but can be smaller.
	idx_t segment_size = block_size < vector_segment_size ? block_size : vector_segment_size;
	allocation_size += segment_size;

	auto &db = GetDatabase();
	auto &config = DBConfig::GetConfig(db);
	auto function = config.GetCompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, type.InternalType());

	auto new_segment = ColumnSegment::CreateTransientSegment(db, *function, type, start_row, segment_size, block_size);
	AppendSegment(l, std::move(new_segment));
}

void ColumnData::UpdateCompressionFunction(SegmentLock &l, const CompressionFunction &function) {
	if (!compression) {
		// compression is empty...
		// if we have no segments - we have not set it yet, so assign it
		// if we have segments, the compression is mixed, so ignore it
		if (data.GetSegmentCount(l) == 0) {
			compression.set(function);
		}
	} else if (compression->type != function.type) {
		// we already have compression set - and we are adding a segment with a different compression
		// compression in the segment is mixed - clear the compression pointer
		compression.reset();
	}
}

void ColumnData::AppendSegment(SegmentLock &l, unique_ptr<ColumnSegment> segment) {
	UpdateCompressionFunction(l, segment->GetCompressionFunction());
	data.AppendSegment(l, std::move(segment));
}

void ColumnData::CommitDropColumn() {
	for (auto &segment_p : data.Segments()) {
		auto &segment = segment_p;
		segment.CommitDropSegment();
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                    PartialBlockManager &partial_block_manager) {
	return make_uniq<ColumnCheckpointState>(row_group, *this, partial_block_manager);
}

void ColumnData::CheckpointScan(ColumnSegment &segment, ColumnScanState &state, idx_t row_group_start, idx_t count,
                                Vector &scan_vector) {
	if (state.scan_options && state.scan_options->force_fetch_row) {
		for (idx_t i = 0; i < count; i++) {
			ColumnFetchState fetch_state;
			segment.FetchRow(fetch_state, UnsafeNumericCast<row_t>(state.row_index + i), scan_vector, i);
		}
	} else {
		segment.Scan(state, count, scan_vector, 0, ScanVectorType::SCAN_FLAT_VECTOR);
	}

	if (updates) {
		D_ASSERT(scan_vector.GetVectorType() == VectorType::FLAT_VECTOR);
		updates->FetchCommittedRange(state.row_index - row_group_start, count, scan_vector);
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::Checkpoint(RowGroup &row_group, ColumnCheckpointInfo &checkpoint_info) {
	// scan the segments of the column data
	// set up the checkpoint state
	auto checkpoint_state = CreateCheckpointState(row_group, checkpoint_info.info.manager);
	checkpoint_state->global_stats = BaseStatistics::CreateEmpty(type).ToUnique();

	auto &nodes = data.ReferenceSegments();
	if (nodes.empty()) {
		// empty table: flush the empty list
		return checkpoint_state;
	}

	vector<reference<ColumnCheckpointState>> states {*checkpoint_state};
	ColumnDataCheckpointer checkpointer(states, GetDatabase(), row_group, checkpoint_info);
	checkpointer.Checkpoint();
	checkpointer.FinalizeCheckpoint();
	return checkpoint_state;
}

void ColumnData::InitializeColumn(PersistentColumnData &column_data) {
	InitializeColumn(column_data, stats->statistics);
}

void ColumnData::InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) {
	D_ASSERT(type.InternalType() == column_data.physical_type);
	// construct the segments based on the data pointers
	this->count = 0;
	for (auto &data_pointer : column_data.pointers) {
		// Update the count and statistics
		this->count += data_pointer.tuple_count;

		// Merge the statistics. If this is a child column, the target_stats reference will point into the parents stats
		// otherwise if this is a top level column, `stats->statistics` == `target_stats`

		target_stats.Merge(data_pointer.statistics);

		// create a persistent segment
		auto segment = ColumnSegment::CreatePersistentSegment(
		    GetDatabase(), block_manager, data_pointer.block_pointer.block_id, data_pointer.block_pointer.offset, type,
		    data_pointer.row_start, data_pointer.tuple_count, data_pointer.compression_type,
		    std::move(data_pointer.statistics), std::move(data_pointer.segment_state));

		auto l = data.Lock();
		AppendSegment(l, std::move(segment));
	}
}

bool ColumnData::IsPersistent() {
	for (auto &segment : data.Segments()) {
		if (segment.segment_type != ColumnSegmentType::PERSISTENT) {
			return false;
		}
	}
	return true;
}

vector<DataPointer> ColumnData::GetDataPointers() {
	vector<DataPointer> pointers;
	for (auto &segment : data.Segments()) {
		pointers.push_back(segment.GetDataPointer());
	}
	return pointers;
}

PersistentColumnData::PersistentColumnData(PhysicalType physical_type_p) : physical_type(physical_type_p) {
}

PersistentColumnData::PersistentColumnData(PhysicalType physical_type, vector<DataPointer> pointers_p)
    : physical_type(physical_type), pointers(std::move(pointers_p)) {
	D_ASSERT(!pointers.empty());
}

PersistentColumnData::~PersistentColumnData() {
}

void PersistentColumnData::Serialize(Serializer &serializer) const {
	if (has_updates) {
		throw InternalException("Column data with updates cannot be serialized");
	}
	serializer.WritePropertyWithDefault(100, "data_pointers", pointers);
	if (child_columns.empty()) {
		// validity column
		D_ASSERT(physical_type == PhysicalType::BIT);
		return;
	}
	serializer.WriteProperty(101, "validity", child_columns[0]);
	if (physical_type == PhysicalType::ARRAY || physical_type == PhysicalType::LIST) {
		D_ASSERT(child_columns.size() == 2);
		serializer.WriteProperty(102, "child_column", child_columns[1]);
	} else if (physical_type == PhysicalType::STRUCT) {
		serializer.WriteList(102, "sub_columns", child_columns.size() - 1,
		                     [&](Serializer::List &list, idx_t i) { list.WriteElement(child_columns[i + 1]); });
	}
}

void PersistentColumnData::DeserializeField(Deserializer &deserializer, field_id_t field_idx, const char *field_name,
                                            const LogicalType &type) {
	deserializer.Set<const LogicalType &>(type);
	child_columns.push_back(deserializer.ReadProperty<PersistentColumnData>(field_idx, field_name));
	deserializer.Unset<LogicalType>();
}

PersistentColumnData PersistentColumnData::Deserialize(Deserializer &deserializer) {
	auto &type = deserializer.Get<const LogicalType &>();
	auto physical_type = type.InternalType();
	PersistentColumnData result(physical_type);
	deserializer.ReadPropertyWithDefault(100, "data_pointers", static_cast<vector<DataPointer> &>(result.pointers));
	if (result.physical_type == PhysicalType::BIT) {
		// validity: return
		return result;
	}
	result.DeserializeField(deserializer, 101, "validity", LogicalTypeId::VALIDITY);
	switch (physical_type) {
	case PhysicalType::ARRAY:
		result.DeserializeField(deserializer, 102, "child_column", ArrayType::GetChildType(type));
		break;
	case PhysicalType::LIST:
		result.DeserializeField(deserializer, 102, "child_column", ListType::GetChildType(type));
		break;
	case PhysicalType::STRUCT: {
		auto &child_types = StructType::GetChildTypes(type);
		deserializer.ReadList(102, "sub_columns", [&](Deserializer::List &list, idx_t i) {
			deserializer.Set<const LogicalType &>(child_types[i].second);
			result.child_columns.push_back(list.ReadElement<PersistentColumnData>());
			deserializer.Unset<LogicalType>();
		});
		break;
	}
	default:
		break;
	}
	return result;
}

bool PersistentColumnData::HasUpdates() const {
	if (has_updates) {
		return true;
	}
	for (auto &child_col : child_columns) {
		if (child_col.HasUpdates()) {
			return true;
		}
	}
	return false;
}

PersistentRowGroupData::PersistentRowGroupData(vector<LogicalType> types_p) : types(std::move(types_p)) {
}

void PersistentRowGroupData::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "types", types);
	serializer.WriteProperty(101, "columns", column_data);
	serializer.WriteProperty(102, "start", start);
	serializer.WriteProperty(103, "count", count);
}

PersistentRowGroupData PersistentRowGroupData::Deserialize(Deserializer &deserializer) {
	PersistentRowGroupData data;
	deserializer.ReadProperty(100, "types", data.types);
	deserializer.ReadList(101, "columns", [&](Deserializer::List &list, idx_t i) {
		deserializer.Set<const LogicalType &>(data.types[i]);
		data.column_data.push_back(list.ReadElement<PersistentColumnData>());
		deserializer.Unset<LogicalType>();
	});
	deserializer.ReadProperty(102, "start", data.start);
	deserializer.ReadProperty(103, "count", data.count);
	return data;
}

bool PersistentRowGroupData::HasUpdates() const {
	for (auto &col : column_data) {
		if (col.HasUpdates()) {
			return true;
		}
	}
	return false;
}

void PersistentCollectionData::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "row_groups", row_group_data);
}

PersistentCollectionData PersistentCollectionData::Deserialize(Deserializer &deserializer) {
	PersistentCollectionData data;
	deserializer.ReadProperty(100, "row_groups", data.row_group_data);
	return data;
}

bool PersistentCollectionData::HasUpdates() const {
	for (auto &row_group : row_group_data) {
		if (row_group.HasUpdates()) {
			return true;
		}
	}
	return false;
}

PersistentColumnData ColumnData::Serialize() {
	PersistentColumnData result(type.InternalType(), GetDataPointers());
	result.has_updates = HasUpdates();
	return result;
}

shared_ptr<ColumnData> ColumnData::Deserialize(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                               idx_t start_row, ReadStream &source, const LogicalType &type) {
	auto entry = ColumnData::CreateColumn(block_manager, info, column_index, start_row, type, nullptr);

	// deserialize the persistent column data
	BinaryDeserializer deserializer(source);
	deserializer.Begin();
	deserializer.Set<DatabaseInstance &>(info.GetDB().GetDatabase());
	CompressionInfo compression_info(block_manager.GetBlockSize());
	deserializer.Set<const CompressionInfo &>(compression_info);
	deserializer.Set<const LogicalType &>(type);
	auto persistent_column_data = PersistentColumnData::Deserialize(deserializer);
	deserializer.Unset<LogicalType>();
	deserializer.Unset<const CompressionInfo>();
	deserializer.Unset<DatabaseInstance>();
	deserializer.End();

	// initialize the column
	entry->InitializeColumn(persistent_column_data, entry->stats->statistics);
	return entry;
}

void ColumnData::GetColumnSegmentInfo(idx_t row_group_index, vector<idx_t> col_path,
                                      vector<ColumnSegmentInfo> &result) {
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
	auto segment = data.GetRootSegment();
	while (segment) {
		ColumnSegmentInfo column_info;
		column_info.row_group_index = row_group_index;
		column_info.column_id = col_path[0];
		column_info.column_path = col_path_str;
		column_info.segment_idx = segment_idx;
		column_info.segment_type = type.ToString();
		column_info.segment_start = segment->start;
		column_info.segment_count = segment->count;
		column_info.compression_type = CompressionTypeToString(segment->GetCompressionFunction().type);
		{
			lock_guard<mutex> l(stats_lock);
			column_info.segment_stats = segment->stats.statistics.ToString();
		}
		column_info.has_updates = ColumnData::HasUpdates();
		// persistent
		// block_id
		// block_offset
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			column_info.persistent = true;
			column_info.block_id = segment->GetBlockId();
			column_info.block_offset = segment->GetBlockOffset();
		} else {
			column_info.persistent = false;
		}
		auto segment_state = segment->GetSegmentState();
		if (segment_state) {
			column_info.segment_info = segment_state->GetSegmentInfo();
			column_info.additional_blocks = segment_state->GetAdditionalBlocks();
		}
		result.emplace_back(column_info);

		segment_idx++;
		segment = data.GetNextSegment(segment);
	}
}

void ColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	D_ASSERT(this->start == parent.start);
	data.Verify();
	if (type.InternalType() == PhysicalType::STRUCT || type.InternalType() == PhysicalType::ARRAY) {
		// structs and fixed size lists don't have segments
		D_ASSERT(!data.GetRootSegment());
		return;
	}
	idx_t current_index = 0;
	idx_t current_start = this->start;
	idx_t total_count = 0;
	for (auto &segment : data.Segments()) {
		D_ASSERT(segment.index == current_index);
		D_ASSERT(segment.start == current_start);
		current_start += segment.count;
		total_count += segment.count;
		current_index++;
	}
	D_ASSERT(this->count == total_count);
#endif
}

template <class RET, class OP>
static RET CreateColumnInternal(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
                                const LogicalType &type, optional_ptr<ColumnData> parent) {
	if (type.InternalType() == PhysicalType::STRUCT) {
		return OP::template Create<StructColumnData>(block_manager, info, column_index, start_row, type, parent);
	} else if (type.InternalType() == PhysicalType::LIST) {
		return OP::template Create<ListColumnData>(block_manager, info, column_index, start_row, type, parent);
	} else if (type.InternalType() == PhysicalType::ARRAY) {
		return OP::template Create<ArrayColumnData>(block_manager, info, column_index, start_row, type, parent);
	} else if (type.id() == LogicalTypeId::VALIDITY) {
		return OP::template Create<ValidityColumnData>(block_manager, info, column_index, start_row, *parent);
	}
	return OP::template Create<StandardColumnData>(block_manager, info, column_index, start_row, type, parent);
}

shared_ptr<ColumnData> ColumnData::CreateColumn(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                                idx_t start_row, const LogicalType &type,
                                                optional_ptr<ColumnData> parent) {
	return CreateColumnInternal<shared_ptr<ColumnData>, SharedConstructor>(block_manager, info, column_index, start_row,
	                                                                       type, parent);
}

unique_ptr<ColumnData> ColumnData::CreateColumnUnique(BlockManager &block_manager, DataTableInfo &info,
                                                      idx_t column_index, idx_t start_row, const LogicalType &type,
                                                      optional_ptr<ColumnData> parent) {
	return CreateColumnInternal<unique_ptr<ColumnData>, UniqueConstructor>(block_manager, info, column_index, start_row,
	                                                                       type, parent);
}

} // namespace duckdb
