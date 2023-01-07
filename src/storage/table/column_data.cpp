#include "duckdb/storage/table/column_data.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/distinct_statistics.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/list_column_data.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/transaction/transaction.hpp"

#include "duckdb/storage/table/struct_column_data.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

ColumnData::ColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
                       LogicalType type, ColumnData *parent)
    : block_manager(block_manager), info(info), column_index(column_index), start(start_row), type(move(type)),
      parent(parent), version(0) {
}

ColumnData::ColumnData(ColumnData &other, idx_t start, ColumnData *parent)
    : block_manager(other.block_manager), info(other.info), column_index(other.column_index), start(start),
      type(move(other.type)), parent(parent), updates(move(other.updates)), version(parent ? parent->version + 1 : 0) {
	idx_t offset = 0;
	for (auto segment = other.data.GetRootSegment(); segment; segment = segment->Next()) {
		auto &other = (ColumnSegment &)*segment;
		this->data.AppendSegment(ColumnSegment::CreateSegment(other, start + offset));
		offset += segment->count;
	}
}

ColumnData::~ColumnData() {
}

DatabaseInstance &ColumnData::GetDatabase() const {
	return info.db.GetDatabase();
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

void ColumnData::IncrementVersion() {
	version++;
}

idx_t ColumnData::GetMaxEntry() {
	auto l = data.Lock();
	auto first_segment = data.GetRootSegment(l);
	auto last_segment = data.GetLastSegment(l);
	if (!first_segment) {
		D_ASSERT(!last_segment);
		return 0;
	} else {
		D_ASSERT(last_segment->start >= first_segment->start);
		return last_segment->start + last_segment->count - first_segment->start;
	}
}

void ColumnData::InitializeScan(ColumnScanState &state) {
	state.current = (ColumnSegment *)data.GetRootSegment();
	state.row_index = state.current ? state.current->start : 0;
	state.internal_index = state.row_index;
	state.initialized = false;
	state.version = version;
	state.scan_state.reset();
}

void ColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	state.current = (ColumnSegment *)data.GetSegment(row_idx);
	state.row_index = row_idx;
	state.internal_index = state.current->start;
	state.initialized = false;
	state.version = version;
	state.scan_state.reset();
}

idx_t ColumnData::ScanVector(ColumnScanState &state, Vector &result, idx_t remaining) {
	state.previous_states.clear();
	if (state.version != version) {
		InitializeScanWithOffset(state, state.row_index);
		state.current->InitializeScan(state);
		state.initialized = true;
	} else if (!state.initialized) {
		D_ASSERT(state.current);
		state.current->InitializeScan(state);
		state.internal_index = state.current->start;
		state.initialized = true;
	}
	D_ASSERT(data.HasSegment(state.current));
	D_ASSERT(state.version == version);
	D_ASSERT(state.internal_index <= state.row_index);
	if (state.internal_index < state.row_index) {
		state.current->Skip(state);
	}
	D_ASSERT(state.current->type == type);
	idx_t initial_remaining = remaining;
	while (remaining > 0) {
		D_ASSERT(state.row_index >= state.current->start &&
		         state.row_index <= state.current->start + state.current->count);
		idx_t scan_count = MinValue<idx_t>(remaining, state.current->start + state.current->count - state.row_index);
		idx_t result_offset = initial_remaining - remaining;
		if (scan_count > 0) {
			state.current->Scan(state, scan_count, result, result_offset, scan_count == initial_remaining);

			state.row_index += scan_count;
			remaining -= scan_count;
		}

		if (remaining > 0) {
			if (!state.current->next) {
				break;
			}
			state.previous_states.emplace_back(move(state.scan_state));
			state.current = (ColumnSegment *)state.current->Next();
			state.current->InitializeScan(state);
			state.segment_checked = false;
			D_ASSERT(state.row_index >= state.current->start &&
			         state.row_index <= state.current->start + state.current->count);
		}
	}
	state.internal_index = state.row_index;
	return initial_remaining - remaining;
}

template <bool SCAN_COMMITTED, bool ALLOW_UPDATES>
idx_t ColumnData::ScanVector(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	auto scan_count = ScanVector(state, result, STANDARD_VECTOR_SIZE);

	lock_guard<mutex> update_guard(update_lock);
	if (updates) {
		if (!ALLOW_UPDATES && updates->HasUncommittedUpdates(vector_index)) {
			throw TransactionException("Cannot create index with outstanding updates");
		}
		result.Flatten(scan_count);
		if (SCAN_COMMITTED) {
			updates->FetchCommitted(vector_index, result);
		} else {
			updates->FetchUpdates(transaction, vector_index, result);
		}
	}
	return scan_count;
}

template idx_t ColumnData::ScanVector<false, false>(TransactionData transaction, idx_t vector_index,
                                                    ColumnScanState &state, Vector &result);
template idx_t ColumnData::ScanVector<true, false>(TransactionData transaction, idx_t vector_index,
                                                   ColumnScanState &state, Vector &result);
template idx_t ColumnData::ScanVector<false, true>(TransactionData transaction, idx_t vector_index,
                                                   ColumnScanState &state, Vector &result);
template idx_t ColumnData::ScanVector<true, true>(TransactionData transaction, idx_t vector_index,
                                                  ColumnScanState &state, Vector &result);

idx_t ColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	return ScanVector<false, true>(transaction, vector_index, state, result);
}

idx_t ColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	if (allow_updates) {
		return ScanVector<true, true>(TransactionData(0, 0), vector_index, state, result);
	} else {
		return ScanVector<true, false>(TransactionData(0, 0), vector_index, state, result);
	}
}

void ColumnData::ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t count, Vector &result) {
	ColumnScanState child_state;
	InitializeScanWithOffset(child_state, row_group_start + offset_in_row_group);
	auto scan_count = ScanVector(child_state, result, count);
	if (updates) {
		result.Flatten(scan_count);
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

void ColumnData::Select(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                        SelectionVector &sel, idx_t &count, const TableFilter &filter) {
	idx_t scan_count = Scan(transaction, vector_index, state, result);
	result.Flatten(scan_count);
	ColumnSegment::FilterSelection(sel, result, filter, count, FlatVector::Validity(result));
}

void ColumnData::FilterScan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
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

void ColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);
	AppendData(stats, state, vdata, count);
}

void ColumnData::InitializeAppend(ColumnAppendState &state) {
	auto l = data.Lock();
	if (data.IsEmpty(l)) {
		// no segments yet, append an empty segment
		AppendTransientSegment(l, start);
	}
	auto segment = (ColumnSegment *)data.GetLastSegment(l);
	if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
		// no transient segments yet
		auto total_rows = segment->start + segment->count;
		AppendTransientSegment(l, total_rows);
		state.current = (ColumnSegment *)data.GetLastSegment(l);
	} else {
		state.current = (ColumnSegment *)segment;
	}

	D_ASSERT(state.current->segment_type == ColumnSegmentType::TRANSIENT);
	state.current->InitializeAppend(state);
	D_ASSERT(state.current->function->append);
}

void ColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata, idx_t count) {
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
			auto l = data.Lock();
			AppendTransientSegment(l, state.current->start + state.current->count);
			state.current = (ColumnSegment *)data.GetLastSegment(l);
			state.current->InitializeAppend(state);
		}
		offset += copied_elements;
		count -= copied_elements;
	}
}

void ColumnData::RevertAppend(row_t start_row) {
	auto l = data.Lock();
	// check if this row is in the segment tree at all
	auto last_segment = data.GetLastSegment(l);
	if (idx_t(start_row) >= last_segment->start + last_segment->count) {
		// the start row is equal to the final portion of the column data: nothing was ever appended here
		D_ASSERT(idx_t(start_row) == last_segment->start + last_segment->count);
		return;
	}
	// find the segment index that the current row belongs to
	idx_t segment_index = data.GetSegmentIndex(l, start_row);
	auto segment = data.GetSegmentByIndex(l, segment_index);
	auto &transient = (ColumnSegment &)*segment;
	D_ASSERT(transient.segment_type == ColumnSegmentType::TRANSIENT);

	// remove any segments AFTER this segment: they should be deleted entirely
	data.EraseSegments(l, segment_index);

	segment->next = nullptr;
	transient.RevertAppend(start_row);
}

idx_t ColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	D_ASSERT(row_id >= 0);
	D_ASSERT(idx_t(row_id) >= start);
	// perform the fetch within the segment
	state.row_index = start + ((row_id - start) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE);
	state.current = (ColumnSegment *)data.GetSegment(state.row_index);
	state.internal_index = state.current->start;
	return ScanVector(state, result, STANDARD_VECTOR_SIZE);
}

void ColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
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

void ColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                        idx_t update_count) {
	lock_guard<mutex> update_guard(update_lock);
	if (!updates) {
		updates = make_unique<UpdateSegment>(*this);
	}
	Vector base_vector(type);
	ColumnScanState state;
	auto fetch_count = Fetch(state, row_ids[0], base_vector);

	base_vector.Flatten(fetch_count);
	updates->Update(transaction, column_index, update_vector, row_ids, update_count, base_vector);
}

void ColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path, Vector &update_vector,
                              row_t *row_ids, idx_t update_count, idx_t depth) {
	// this method should only be called at the end of the path in the base column case
	D_ASSERT(depth >= column_path.size());
	ColumnData::Update(transaction, column_path[0], update_vector, row_ids, update_count);
}

unique_ptr<BaseStatistics> ColumnData::GetUpdateStatistics() {
	lock_guard<mutex> update_guard(update_lock);
	return updates ? updates->GetStatistics() : nullptr;
}

void ColumnData::AppendTransientSegment(SegmentLock &l, idx_t start_row) {
	idx_t segment_size = Storage::BLOCK_SIZE;
	if (start_row == idx_t(MAX_ROW_ID)) {
#if STANDARD_VECTOR_SIZE < 1024
		segment_size = 1024 * GetTypeIdSize(type.InternalType());
#else
		segment_size = STANDARD_VECTOR_SIZE * GetTypeIdSize(type.InternalType());
#endif
	}
	auto new_segment = ColumnSegment::CreateTransientSegment(GetDatabase(), type, start_row, segment_size);
	data.AppendSegment(l, move(new_segment));
}

void ColumnData::CommitDropColumn() {
	auto segment = (ColumnSegment *)data.GetRootSegment();
	while (segment) {
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			auto block_id = segment->GetBlockId();
			if (block_id != INVALID_BLOCK) {
				block_manager.MarkBlockAsModified(block_id);
			}
		}
		segment = (ColumnSegment *)segment->Next();
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                    PartialBlockManager &partial_block_manager) {
	return make_unique<ColumnCheckpointState>(row_group, *this, partial_block_manager);
}

void ColumnData::CheckpointScan(ColumnSegment *segment, ColumnScanState &state, idx_t row_group_start, idx_t count,
                                Vector &scan_vector) {
	segment->Scan(state, count, scan_vector, 0, true);
	if (updates) {
		scan_vector.Flatten(count);
		updates->FetchCommittedRange(state.row_index - row_group_start, count, scan_vector);
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::Checkpoint(RowGroup &row_group,
                                                         PartialBlockManager &partial_block_manager,
                                                         ColumnCheckpointInfo &checkpoint_info) {
	// scan the segments of the column data
	// set up the checkpoint state
	auto checkpoint_state = CreateCheckpointState(row_group, partial_block_manager);
	checkpoint_state->global_stats = BaseStatistics::CreateEmpty(type, StatisticsType::LOCAL_STATS);

	auto l = data.Lock();
	auto nodes = data.MoveSegments(l);
	if (nodes.empty()) {
		// empty table: flush the empty list
		return checkpoint_state;
	}
	lock_guard<mutex> update_guard(update_lock);

	ColumnDataCheckpointer checkpointer(*this, row_group, *checkpoint_state, checkpoint_info);
	checkpointer.Checkpoint(move(nodes));

	// replace the old tree with the new one
	data.Replace(l, checkpoint_state->new_tree);
	version++;

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
		data_pointer.compression_type = source.Read<CompressionType>();
		data_pointer.statistics = BaseStatistics::Deserialize(source, type);

		// create a persistent segment
		auto segment = ColumnSegment::CreatePersistentSegment(
		    GetDatabase(), block_manager, data_pointer.block_pointer.block_id, data_pointer.block_pointer.offset, type,
		    data_pointer.row_start, data_pointer.tuple_count, data_pointer.compression_type,
		    move(data_pointer.statistics));
		data.AppendSegment(move(segment));
	}
}

shared_ptr<ColumnData> ColumnData::Deserialize(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                               idx_t start_row, Deserializer &source, const LogicalType &type,
                                               ColumnData *parent) {
	auto entry = ColumnData::CreateColumn(block_manager, info, column_index, start_row, type, parent);
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
		// compression
		column_info.emplace_back(CompressionTypeToString(segment->function->type));
		// stats
		column_info.emplace_back(segment->stats.statistics ? segment->stats.statistics->ToString()
		                                                   : string("No Stats"));
		// has_updates
		column_info.push_back(Value::BOOLEAN(updates ? true : false));
		// persistent
		// block_id
		// block_offset
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			column_info.push_back(Value::BOOLEAN(true));
			column_info.push_back(Value::BIGINT(segment->GetBlockId()));
			column_info.push_back(Value::BIGINT(segment->GetBlockOffset()));
		} else {
			column_info.push_back(Value::BOOLEAN(false));
			column_info.emplace_back();
			column_info.emplace_back();
		}

		result.push_back(move(column_info));

		segment_idx++;
		segment = (ColumnSegment *)segment->Next();
	}
}

void ColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	D_ASSERT(this->start == parent.start);
	data.Verify();
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
			root = root->Next();
		}
	}
#endif
}

template <class RET, class OP>
static RET CreateColumnInternal(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
                                const LogicalType &type, ColumnData *parent) {
	if (type.InternalType() == PhysicalType::STRUCT) {
		return OP::template Create<StructColumnData>(block_manager, info, column_index, start_row, type, parent);
	} else if (type.InternalType() == PhysicalType::LIST) {
		return OP::template Create<ListColumnData>(block_manager, info, column_index, start_row, type, parent);
	} else if (type.id() == LogicalTypeId::VALIDITY) {
		return OP::template Create<ValidityColumnData>(block_manager, info, column_index, start_row, parent);
	}
	return OP::template Create<StandardColumnData>(block_manager, info, column_index, start_row, type, parent);
}

template <class RET, class OP>
static RET CreateColumnInternal(ColumnData &other, idx_t start_row, ColumnData *parent) {
	if (other.type.InternalType() == PhysicalType::STRUCT) {
		return OP::template Create<StructColumnData>(other, start_row, parent);
	} else if (other.type.InternalType() == PhysicalType::LIST) {
		return OP::template Create<ListColumnData>(other, start_row, parent);
	} else if (other.type.id() == LogicalTypeId::VALIDITY) {
		return OP::template Create<ValidityColumnData>(other, start_row, parent);
	}
	return OP::template Create<StandardColumnData>(other, start_row, parent);
}

shared_ptr<ColumnData> ColumnData::CreateColumn(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                                idx_t start_row, const LogicalType &type, ColumnData *parent) {
	return CreateColumnInternal<shared_ptr<ColumnData>, SharedConstructor>(block_manager, info, column_index, start_row,
	                                                                       type, parent);
}

shared_ptr<ColumnData> ColumnData::CreateColumn(ColumnData &other, idx_t start_row, ColumnData *parent) {
	return CreateColumnInternal<shared_ptr<ColumnData>, SharedConstructor>(other, start_row, parent);
}

unique_ptr<ColumnData> ColumnData::CreateColumnUnique(BlockManager &block_manager, DataTableInfo &info,
                                                      idx_t column_index, idx_t start_row, const LogicalType &type,
                                                      ColumnData *parent) {
	return CreateColumnInternal<unique_ptr<ColumnData>, UniqueConstructor>(block_manager, info, column_index, start_row,
	                                                                       type, parent);
}

unique_ptr<ColumnData> ColumnData::CreateColumnUnique(ColumnData &other, idx_t start_row, ColumnData *parent) {
	return CreateColumnInternal<unique_ptr<ColumnData>, UniqueConstructor>(other, start_row, parent);
}

} // namespace duckdb
