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

ColumnData::ColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, LogicalType type_p,
                       ColumnDataType data_type_p, optional_ptr<ColumnData> parent_p)
    : count(0), block_manager(block_manager), info(info), column_index(column_index), type(std::move(type_p)),
      allocation_size(0),
      data_type(data_type_p == ColumnDataType::CHECKPOINT_TARGET ? ColumnDataType::MAIN_TABLE : data_type_p),
      parent(parent_p) {
	if (!parent) {
		stats = make_uniq<SegmentStatistics>(type);
	}
}

ColumnData::~ColumnData() {
}

void ColumnData::SetDataType(ColumnDataType data_type_p) {
	this->data_type = data_type_p;
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

bool ColumnData::HasChanges() const {
	for (auto &segment_node : data.SegmentNodes()) {
		auto &segment = *segment_node.node;
		if (segment.segment_type == ColumnSegmentType::TRANSIENT) {
			// transient segment: always need to write to disk
			return true;
		}
		// persistent segment; check if there were any updates or deletions in this segment
		idx_t start_row_idx = segment_node.row_start;
		idx_t end_row_idx = start_row_idx + segment.count;
		if (HasChanges(start_row_idx, end_row_idx)) {
			return true;
		}
	}
	return false;
}

bool ColumnData::HasAnyChanges() const {
	return HasChanges();
}

idx_t ColumnData::GetMaxEntry() {
	return count;
}

void ColumnData::InitializeScan(ColumnScanState &state) {
	state.current = data.GetRootSegment();
	state.segment_tree = &data;
	state.offset_in_column = state.current ? state.current->row_start : 0;
	state.internal_index = state.offset_in_column;
	state.initialized = false;
	state.scan_state.reset();
	state.last_offset = 0;
}

void ColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	if (row_idx > count) {
		throw InternalException("row_idx in InitializeScanWithOffset out of range");
	}
	state.current = data.GetSegment(row_idx);
	state.segment_tree = &data;
	state.offset_in_column = row_idx;
	state.internal_index = state.current->row_start;
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
	auto &current = *state.current->node;
	idx_t remaining_in_segment = state.current->row_start + current.count - state.offset_in_column;
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
		current_segment->node->InitializePrefetch(prefetch_state, scan_state);
	}
	idx_t row_index = scan_state.offset_in_column;
	while (remaining > 0) {
		auto &current = *current_segment->node;
		idx_t scan_count = MinValue<idx_t>(remaining, current_segment->row_start + current.count - row_index);
		remaining -= scan_count;
		row_index += scan_count;
		if (remaining > 0) {
			auto next = data.GetNextSegment(*current_segment);
			if (!next) {
				break;
			}
			next->node->InitializePrefetch(prefetch_state, scan_state);
			current_segment = next;
		}
	}
}

void ColumnData::BeginScanVectorInternal(ColumnScanState &state) {
	state.previous_states.clear();
	if (!state.initialized) {
		auto &current = *state.current->node;
		current.InitializeScan(state);
		state.internal_index = state.current->row_start;
		state.initialized = true;
	}
	D_ASSERT(data.HasSegment(*state.current));
	D_ASSERT(state.internal_index <= state.offset_in_column);
	if (state.internal_index < state.offset_in_column) {
		auto &current = *state.current->node;
		current.Skip(state);
	}
	D_ASSERT(state.current->node->type == type);
}

idx_t ColumnData::ScanVector(ColumnScanState &state, Vector &result, idx_t remaining, ScanVectorType scan_type,
                             idx_t base_result_offset) {
	if (scan_type == ScanVectorType::SCAN_FLAT_VECTOR && result.GetVectorType() != VectorType::FLAT_VECTOR) {
		throw InternalException("ScanVector called with SCAN_FLAT_VECTOR but result is not a flat vector");
	}
	BeginScanVectorInternal(state);
	idx_t initial_remaining = remaining;
	while (remaining > 0) {
		auto &current = *state.current->node;
		auto current_start = state.current->row_start;
		D_ASSERT(state.offset_in_column >= current_start && state.offset_in_column <= current_start + current.count);
		idx_t scan_count = MinValue<idx_t>(remaining, current_start + current.count - state.offset_in_column);
		idx_t result_offset = base_result_offset + initial_remaining - remaining;
		if (scan_count > 0) {
			if (state.scan_options && state.scan_options->force_fetch_row) {
				for (idx_t i = 0; i < scan_count; i++) {
					ColumnFetchState fetch_state;
					current.FetchRow(fetch_state, UnsafeNumericCast<row_t>(state.offset_in_column + i - current_start),
					                 result, result_offset + i);
				}
			} else {
				current.Scan(state, scan_count, result, result_offset, scan_type);
			}

			state.offset_in_column += scan_count;
			remaining -= scan_count;
		}

		if (remaining > 0) {
			auto next = data.GetNextSegment(*state.current);
			if (!next) {
				break;
			}
			state.previous_states.emplace_back(std::move(state.scan_state));
			state.current = next;
			state.current->node->InitializeScan(state);
			state.segment_checked = false;
			D_ASSERT(state.offset_in_column >= state.current->row_start &&
			         state.offset_in_column <= state.current->row_start + state.current->node->count);
		}
	}
	state.internal_index = state.offset_in_column;
	return initial_remaining - remaining;
}

void ColumnData::SelectVector(ColumnScanState &state, Vector &result, idx_t target_count, const SelectionVector &sel,
                              idx_t sel_count) {
	BeginScanVectorInternal(state);
	auto &current = *state.current->node;
	if (state.current->row_start + current.count - state.offset_in_column < target_count) {
		throw InternalException("ColumnData::SelectVector should be able to fetch everything from one segment");
	}
	if (state.scan_options && state.scan_options->force_fetch_row) {
		for (idx_t i = 0; i < sel_count; i++) {
			auto source_idx = sel.get_index(i);
			ColumnFetchState fetch_state;
			current.FetchRow(fetch_state, UnsafeNumericCast<row_t>(state.offset_in_column + source_idx), result, i);
		}
	} else {
		current.Select(state, target_count, result, sel, sel_count);
	}
	state.offset_in_column += target_count;
	state.internal_index = state.offset_in_column;
}

void ColumnData::FilterVector(ColumnScanState &state, Vector &result, idx_t target_count, SelectionVector &sel,
                              idx_t &sel_count, const TableFilter &filter, TableFilterState &filter_state) {
	BeginScanVectorInternal(state);
	auto &current = *state.current->node;
	if (state.current->row_start + current.count - state.offset_in_column < target_count) {
		throw InternalException("ColumnData::Filter should be able to fetch everything from one segment");
	}
	current.Filter(state, target_count, result, sel, sel_count, filter, filter_state);
	state.offset_in_column += target_count;
	state.internal_index = state.offset_in_column;
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

void ColumnData::UpdateInternal(TransactionData transaction, DataTable &data_table, idx_t column_index,
                                Vector &update_vector, row_t *row_ids, idx_t update_count, Vector &base_vector,
                                idx_t row_group_start) {
	lock_guard<mutex> update_guard(update_lock);
	if (!updates) {
		updates = make_uniq<UpdateSegment>(*this);
	}
	updates->Update(transaction, data_table, column_index, update_vector, row_ids, update_count, base_vector,
	                row_group_start);
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
	ColumnScanState child_state(nullptr);
	InitializeScanWithOffset(child_state, offset_in_row_group);
	bool has_updates = HasUpdates();
	auto scan_count = ScanVector(child_state, result, s_count, ScanVectorType::SCAN_FLAT_VECTOR);
	if (has_updates) {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		result.Flatten(scan_count);
		updates->FetchCommittedRange(offset_in_row_group, s_count, result);
	}
}

idx_t ColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t scan_count, idx_t result_offset) {
	if (scan_count == 0) {
		return 0;
	}
	// ScanCount can only be used if there are no updates
	D_ASSERT(!HasUpdates());
	return ScanVector(state, result, scan_count, ScanVectorType::SCAN_FLAT_VECTOR, result_offset);
}

void ColumnData::Filter(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                        SelectionVector &sel, idx_t &s_count, const TableFilter &filter,
                        TableFilterState &filter_state) {
	idx_t scan_count = Scan(transaction, vector_index, state, result);

	UnifiedVectorFormat vdata;
	result.ToUnifiedFormat(scan_count, vdata);
	ColumnSegment::FilterSelection(sel, result, vdata, filter, filter_state, scan_count, s_count);
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
	if (!stats) {
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
	// for dynamic filters we never consider the segment being "checked" as it can always change
	state.segment_checked = filter.filter_type != TableFilterType::DYNAMIC_FILTER;
	FilterPropagateResult prune_result;
	{
		lock_guard<mutex> l(stats_lock);
		prune_result = filter.CheckStatistics(state.current->node->stats.statistics);
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
		AppendTransientSegment(l, 0);
	}
	auto segment = data.GetLastSegment(l);
	auto &last_segment = *segment->node;
	if (last_segment.segment_type == ColumnSegmentType::PERSISTENT ||
	    !last_segment.GetCompressionFunction().init_append) {
		// we cannot append to this segment - append a new segment
		auto total_rows = segment->row_start + last_segment.count;
		AppendTransientSegment(l, total_rows);
		state.current = data.GetLastSegment(l);
	} else {
		state.current = segment;
	}
	auto &append_segment = *state.current->node;
	D_ASSERT(append_segment.segment_type == ColumnSegmentType::TRANSIENT);
	append_segment.InitializeAppend(state);
	D_ASSERT(append_segment.GetCompressionFunction().append);
}

void ColumnData::AppendData(BaseStatistics &append_stats, ColumnAppendState &state, UnifiedVectorFormat &vdata,
                            idx_t append_count) {
	idx_t offset = 0;
	while (true) {
		// append the data from the vector
		auto &append_segment = *state.current->node;
		idx_t copied_elements = append_segment.Append(state, vdata, offset, append_count);
		this->count += copied_elements;
		append_stats.Merge(append_segment.stats.statistics);
		if (copied_elements == append_count) {
			// finished copying everything
			break;
		}

		// we couldn't fit everything we wanted in the current column segment, create a new one
		{
			auto l = data.Lock();
			AppendTransientSegment(l, state.current->row_start + append_segment.count);
			state.current = data.GetLastSegment(l);
			state.current->node->InitializeAppend(state);
		}
		offset += copied_elements;
		append_count -= copied_elements;
	}
}

void ColumnData::RevertAppend(row_t new_count_p) {
	idx_t new_count = NumericCast<idx_t>(new_count_p);
	auto l = data.Lock();
	// check if this row is in the segment tree at all
	auto last_segment_node = data.GetLastSegment(l);
	if (!last_segment_node) {
		return;
	}
	auto &last_segment = *last_segment_node->node;
	if (new_count >= last_segment_node->row_start + last_segment.count) {
		// the start row is equal to the final portion of the column data: nothing was ever appended here
		D_ASSERT(new_count == last_segment_node->row_start + last_segment.count);
		return;
	}
	// find the segment index that the current row belongs to
	idx_t segment_index = data.GetSegmentIndex(l, new_count);
	auto segment = data.GetSegmentByIndex(l, UnsafeNumericCast<int64_t>(segment_index));
	if (segment->row_start == new_count) {
		// we are truncating exactly this segment - erase it entirely
		data.EraseSegments(l, segment_index);
	} else {
		// we need to truncate within the segment
		// remove any segments AFTER this segment: they should be deleted entirely
		data.EraseSegments(l, segment_index + 1);

		auto &transient = *segment->node;
		D_ASSERT(transient.segment_type == ColumnSegmentType::TRANSIENT);
		segment->next = nullptr;
		transient.RevertAppend(new_count - segment->row_start);
	}

	this->count = new_count;
}

idx_t ColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	if (UnsafeNumericCast<idx_t>(row_id) > count) {
		throw InternalException("ColumnData::Fetch - row_id out of range");
	}
	D_ASSERT(row_id >= 0);
	// perform the fetch within the segment
	state.offset_in_column = UnsafeNumericCast<idx_t>(row_id) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE;
	state.current = data.GetSegment(state.offset_in_column);
	state.internal_index = state.current->row_start;
	return ScanVector(state, result, STANDARD_VECTOR_SIZE, ScanVectorType::SCAN_FLAT_VECTOR);
}

void ColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                          idx_t result_idx) {
	if (UnsafeNumericCast<idx_t>(row_id) > count) {
		throw InternalException("ColumnData::FetchRow - row_id out of range");
	}
	auto segment = data.GetSegment(UnsafeNumericCast<idx_t>(row_id));

	// now perform the fetch within the segment
	auto index_in_segment = row_id - UnsafeNumericCast<row_t>(segment->row_start);
	segment->node->FetchRow(state, index_in_segment, result, result_idx);
	// merge any updates made to this row

	FetchUpdateRow(transaction, row_id, result, result_idx);
}

idx_t ColumnData::FetchUpdateData(ColumnScanState &state, row_t *row_ids, Vector &base_vector, idx_t row_group_start) {
	if (row_ids[0] < UnsafeNumericCast<row_t>(row_group_start)) {
		throw InternalException("ColumnData::FetchUpdateData out of range");
	}
	auto fetch_count = ColumnData::Fetch(state, row_ids[0] - UnsafeNumericCast<row_t>(row_group_start), base_vector);
	base_vector.Flatten(fetch_count);
	return fetch_count;
}

void ColumnData::Update(TransactionData transaction, DataTable &data_table, idx_t column_index, Vector &update_vector,
                        row_t *row_ids, idx_t update_count, idx_t row_group_start) {
	Vector base_vector(type);
	ColumnScanState state(nullptr);
	FetchUpdateData(state, row_ids, base_vector, row_group_start);

	UpdateInternal(transaction, data_table, column_index, update_vector, row_ids, update_count, base_vector,
	               row_group_start);
}

void ColumnData::UpdateColumn(TransactionData transaction, DataTable &data_table, const vector<column_t> &column_path,
                              Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth,
                              idx_t row_group_start) {
	// this method should only be called at the end of the path in the base column case
	D_ASSERT(depth >= column_path.size());
	ColumnData::Update(transaction, data_table, column_path[0], update_vector, row_ids, update_count, row_group_start);
}

void ColumnData::AppendTransientSegment(SegmentLock &l, idx_t start_row) {
	const auto block_size = block_manager.GetBlockSize();
	const auto type_size = GetTypeIdSize(type.InternalType());
	auto vector_segment_size = block_size;

	if (data_type == ColumnDataType::INITIAL_TRANSACTION_LOCAL && start_row == 0) {
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

	auto new_segment = ColumnSegment::CreateTransientSegment(db, *function, type, segment_size, block_manager);
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

unique_ptr<ColumnCheckpointState> ColumnData::CreateCheckpointState(const RowGroup &row_group,
                                                                    PartialBlockManager &partial_block_manager) {
	return make_uniq<ColumnCheckpointState>(row_group, *this, partial_block_manager);
}

void ColumnData::CheckpointScan(ColumnSegment &segment, ColumnScanState &state, idx_t count,
                                Vector &scan_vector) const {
	if (state.scan_options && state.scan_options->force_fetch_row) {
		for (idx_t i = 0; i < count; i++) {
			ColumnFetchState fetch_state;
			fetch_state.row_group = state.parent->row_group;
			segment.FetchRow(fetch_state, UnsafeNumericCast<row_t>(state.offset_in_column + i), scan_vector, i);
		}
	} else {
		segment.Scan(state, count, scan_vector, 0, ScanVectorType::SCAN_FLAT_VECTOR);
	}

	if (updates) {
		D_ASSERT(scan_vector.GetVectorType() == VectorType::FLAT_VECTOR);
		updates->FetchCommittedRange(state.offset_in_column, count, scan_vector);
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::Checkpoint(const RowGroup &row_group,
                                                         ColumnCheckpointInfo &checkpoint_info) {
	// scan the segments of the column data
	// set up the checkpoint state
	auto &partial_block_manager = checkpoint_info.GetPartialBlockManager();
	auto checkpoint_state = CreateCheckpointState(row_group, partial_block_manager);
	checkpoint_state->global_stats = BaseStatistics::CreateEmpty(type).ToUnique();

	if (!data.GetRootSegment()) {
		// empty table: flush the empty list
		return checkpoint_state;
	}

	vector<reference<ColumnCheckpointState>> states {*checkpoint_state};
	ColumnDataCheckpointer checkpointer(states, GetStorageManager(), row_group, checkpoint_info);
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
		data_pointer.row_start = count;
		this->count += data_pointer.tuple_count;

		// Merge the statistics. If this is a child column, the target_stats reference will point into the parents stats
		// otherwise if this is a top level column, `stats->statistics` == `target_stats`

		target_stats.Merge(data_pointer.statistics);

		// create a persistent segment
		auto segment = ColumnSegment::CreatePersistentSegment(
		    GetDatabase(), block_manager, data_pointer.block_pointer.block_id, data_pointer.block_pointer.offset, type,
		    data_pointer.tuple_count, data_pointer.compression_type, std::move(data_pointer.statistics),
		    std::move(data_pointer.segment_state));

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
	idx_t row_start = 0;
	for (auto &segment : data.Segments()) {
		pointers.push_back(segment.GetDataPointer(row_start));
		row_start += segment.count;
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
	auto result = count ? PersistentColumnData(type.InternalType(), GetDataPointers())
	                    : PersistentColumnData(type.InternalType());
	result.has_updates = HasUpdates();
	return result;
}

shared_ptr<ColumnData> ColumnData::Deserialize(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                               ReadStream &source, const LogicalType &type) {
	auto entry = ColumnData::CreateColumn(block_manager, info, column_index, type);

	// deserialize the persistent column data
	BinaryDeserializer deserializer(source);
	deserializer.Begin();
	deserializer.Set<DatabaseInstance &>(info.GetDB().GetDatabase());
	CompressionInfo compression_info(block_manager);
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

void ColumnData::GetColumnSegmentInfo(const QueryContext &context, idx_t row_group_index, vector<idx_t> col_path,
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
	for (auto &segment_node : data.SegmentNodes()) {
		auto &segment = *segment_node.node;
		ColumnSegmentInfo column_info;
		column_info.row_group_index = row_group_index;
		column_info.column_id = col_path[0];
		column_info.column_path = col_path_str;
		column_info.segment_idx = segment_idx;
		column_info.segment_type = type.ToString();
		column_info.segment_start = segment_node.row_start;
		column_info.segment_count = segment.count;
		column_info.compression_type = CompressionTypeToString(segment.GetCompressionFunction().type);
		{
			lock_guard<mutex> l(stats_lock);
			column_info.segment_stats = segment.stats.statistics.ToString();
		}
		column_info.has_updates = ColumnData::HasUpdates();
		// persistent
		// block_id
		// block_offset
		if (segment.segment_type == ColumnSegmentType::PERSISTENT) {
			column_info.persistent = true;
			column_info.block_id = segment.GetBlockId();
			column_info.block_offset = segment.GetBlockOffset();
		} else {
			column_info.persistent = false;
		}
		auto &compression_function = segment.GetCompressionFunction();
		auto segment_state = segment.GetSegmentState();
		if (segment_state) {
			column_info.segment_info = segment_state->GetSegmentInfo();
			column_info.additional_blocks = segment_state->GetAdditionalBlocks();
		}
		if (compression_function.get_segment_info) {
			auto segment_info = compression_function.get_segment_info(context, segment);
			vector<string> sinfo;
			for (auto &item : segment_info) {
				auto &mode = item.first;
				auto &count = item.second;
				sinfo.push_back(StringUtil::Format("%s: %s", mode, count));
			}
			column_info.segment_info = StringUtil::Join(sinfo, ", ");
		}
		result.emplace_back(column_info);

		segment_idx++;
	}
}

void ColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	data.Verify();
	if (type.InternalType() == PhysicalType::STRUCT || type.InternalType() == PhysicalType::ARRAY) {
		// structs and fixed size lists don't have segments
		D_ASSERT(!data.GetRootSegment());
		return;
	}
	idx_t current_index = 0;
	idx_t current_start = 0;
	idx_t total_count = 0;
	for (auto &segment : data.SegmentNodes()) {
		D_ASSERT(segment.index == current_index);
		D_ASSERT(segment.row_start == current_start);
		current_start += segment.node->count;
		total_count += segment.node->count;
		current_index++;
	}
	D_ASSERT(this->count == total_count);
#endif
}

shared_ptr<ColumnData> ColumnData::CreateColumn(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                                const LogicalType &type, ColumnDataType data_type,
                                                optional_ptr<ColumnData> parent) {
	if (type.InternalType() == PhysicalType::STRUCT) {
		return make_shared_ptr<StructColumnData>(block_manager, info, column_index, type, data_type, parent);
	} else if (type.InternalType() == PhysicalType::LIST) {
		return make_shared_ptr<ListColumnData>(block_manager, info, column_index, type, data_type, parent);
	} else if (type.InternalType() == PhysicalType::ARRAY) {
		return make_shared_ptr<ArrayColumnData>(block_manager, info, column_index, type, data_type, parent);
	} else if (type.id() == LogicalTypeId::VALIDITY) {
		return make_shared_ptr<ValidityColumnData>(block_manager, info, column_index, data_type, parent);
	}
	return make_shared_ptr<StandardColumnData>(block_manager, info, column_index, type, data_type, parent);
}

} // namespace duckdb
