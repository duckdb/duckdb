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
#include "duckdb/storage/table/struct_column_data.hpp"
#include "duckdb/storage/table/update_segment.hpp"

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
	state.internal_index = state.row_index;
	state.initialized = false;
}

void ColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	state.current = (ColumnSegment *)data.GetSegment(row_idx);
	state.row_index = row_idx;
	state.internal_index = state.current->start;
	state.initialized = false;
}

idx_t ColumnData::ScanVector(ColumnScanState &state, Vector &result, idx_t remaining) {
	if (!state.initialized) {
		D_ASSERT(state.current);
		state.current->InitializeScan(state);
		state.internal_index = state.current->start;
		state.initialized = true;
	}
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
		state.current->Scan(state, scan_count, result, result_offset, scan_count == initial_remaining);

		state.row_index += scan_count;
		remaining -= scan_count;
		if (remaining > 0) {
			if (!state.current->next) {
				break;
			}
			state.current = (ColumnSegment *)state.current->next.get();
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
	ColumnSegment::FilterSelection(sel, result, filter, count, FlatVector::Validity(result));
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
		state.current = (ColumnSegment *)data.GetLastSegment();
	} else {
		state.current = (ColumnSegment *)segment;
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
			state.current = (ColumnSegment *)data.GetLastSegment();
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
	auto &transient = (ColumnSegment &)*segment;
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
	state.internal_index = state.current->start;
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
                        idx_t update_count) {
	lock_guard<mutex> update_guard(update_lock);
	if (!updates) {
		updates = make_unique<UpdateSegment>(*this);
	}
	Vector base_vector(type);
	ColumnScanState state;
	auto fetch_count = Fetch(state, row_ids[0], base_vector);

	base_vector.Normalify(fetch_count);
	updates->Update(transaction, column_index, update_vector, row_ids, update_count, base_vector);
}

void ColumnData::UpdateColumn(Transaction &transaction, const vector<column_t> &column_path, Vector &update_vector,
                              row_t *row_ids, idx_t update_count, idx_t depth) {
	// this method should only be called at the end of the path in the base column case
	D_ASSERT(depth >= column_path.size());
	ColumnData::Update(transaction, column_path[0], update_vector, row_ids, update_count);
}

unique_ptr<BaseStatistics> ColumnData::GetUpdateStatistics() {
	lock_guard<mutex> update_guard(update_lock);
	return updates ? updates->GetStatistics() : nullptr;
}

void ColumnData::AppendTransientSegment(idx_t start_row) {
	auto new_segment = ColumnSegment::CreateTransientSegment(GetDatabase(), type, start_row);
	data.AppendSegment(move(new_segment));
}

void ColumnData::CommitDropColumn() {
	auto &block_manager = BlockManager::GetBlockManager(GetDatabase());
	auto segment = (ColumnSegment *)data.GetRootSegment();
	while (segment) {
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			auto block_id = segment->GetBlockId();
			if (block_id != INVALID_BLOCK) {
				block_manager.MarkBlockAsModified(block_id);
			}
		}
		segment = (ColumnSegment *)segment->next.get();
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::CreateCheckpointState(RowGroup &row_group, TableDataWriter &writer) {
	return make_unique<ColumnCheckpointState>(row_group, *this, writer);
}

void ColumnData::CheckpointScan(ColumnSegment *segment, ColumnScanState &state, idx_t row_group_start, idx_t count,
                                Vector &scan_vector) {
	segment->Scan(state, count, scan_vector, 0, true);
	if (updates) {
		scan_vector.Normalify(count);
		updates->FetchCommittedRange(state.row_index - row_group_start, count, scan_vector);
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::Checkpoint(RowGroup &row_group, TableDataWriter &writer,
                                                         ColumnCheckpointInfo &checkpoint_info) {
	// scan the segments of the column data
	// set up the checkpoint state
	auto checkpoint_state = CreateCheckpointState(row_group, writer);
	checkpoint_state->global_stats = BaseStatistics::CreateEmpty(type, StatisticsType::LOCAL_STATS);

	if (!data.root_node) {
		// empty table: flush the empty list
		return checkpoint_state;
	}
	lock_guard<mutex> update_guard(update_lock);

	ColumnDataCheckpointer checkpointer(*this, row_group, *checkpoint_state, checkpoint_info);
	checkpointer.Checkpoint(move(data.root_node));

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
		data_pointer.compression_type = source.Read<CompressionType>();
		data_pointer.statistics = BaseStatistics::Deserialize(source, type);

		// create a persistent segment
		auto segment = ColumnSegment::CreatePersistentSegment(
		    GetDatabase(), data_pointer.block_pointer.block_id, data_pointer.block_pointer.offset, type,
		    data_pointer.row_start, data_pointer.tuple_count, data_pointer.compression_type,
		    move(data_pointer.statistics));
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
