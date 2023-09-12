#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Row Group Segment Tree
//===--------------------------------------------------------------------===//
RowGroupSegmentTree::RowGroupSegmentTree(RowGroupCollection &collection)
    : SegmentTree<RowGroup, true>(), collection(collection), current_row_group(0), max_row_group(0) {
}
RowGroupSegmentTree::~RowGroupSegmentTree() {
}

void RowGroupSegmentTree::Initialize(PersistentTableData &data) {
	D_ASSERT(data.row_group_count > 0);
	current_row_group = 0;
	max_row_group = data.row_group_count;
	finished_loading = false;
	reader = make_uniq<MetadataReader>(collection.GetMetadataManager(), data.block_pointer);
}

unique_ptr<RowGroup> RowGroupSegmentTree::LoadSegment() {
	if (current_row_group >= max_row_group) {
		finished_loading = true;
		return nullptr;
	}
	BinaryDeserializer deserializer(*reader);
	deserializer.Begin();
	auto row_group_pointer = RowGroup::Deserialize(deserializer);
	deserializer.End();
	current_row_group++;
	return make_uniq<RowGroup>(collection, std::move(row_group_pointer));
}

//===--------------------------------------------------------------------===//
// Row Group Collection
//===--------------------------------------------------------------------===//
RowGroupCollection::RowGroupCollection(shared_ptr<DataTableInfo> info_p, BlockManager &block_manager,
                                       vector<LogicalType> types_p, idx_t row_start_p, idx_t total_rows_p)
    : block_manager(block_manager), total_rows(total_rows_p), info(std::move(info_p)), types(std::move(types_p)),
      row_start(row_start_p) {
	row_groups = make_shared<RowGroupSegmentTree>(*this);
}

idx_t RowGroupCollection::GetTotalRows() const {
	return total_rows.load();
}

const vector<LogicalType> &RowGroupCollection::GetTypes() const {
	return types;
}

Allocator &RowGroupCollection::GetAllocator() const {
	return Allocator::Get(info->db);
}

AttachedDatabase &RowGroupCollection::GetAttached() {
	return GetTableInfo().db;
}

MetadataManager &RowGroupCollection::GetMetadataManager() {
	return GetBlockManager().GetMetadataManager();
}

//===--------------------------------------------------------------------===//
// Initialize
//===--------------------------------------------------------------------===//
void RowGroupCollection::Initialize(PersistentTableData &data) {
	D_ASSERT(this->row_start == 0);
	auto l = row_groups->Lock();
	this->total_rows = data.total_rows;
	row_groups->Initialize(data);
	stats.Initialize(types, data);
}

void RowGroupCollection::InitializeEmpty() {
	stats.InitializeEmpty(types);
}

void RowGroupCollection::AppendRowGroup(SegmentLock &l, idx_t start_row) {
	D_ASSERT(start_row >= row_start);
	auto new_row_group = make_uniq<RowGroup>(*this, start_row, 0);
	new_row_group->InitializeEmpty(types);
	row_groups->AppendSegment(l, std::move(new_row_group));
}

RowGroup *RowGroupCollection::GetRowGroup(int64_t index) {
	return (RowGroup *)row_groups->GetSegmentByIndex(index);
}

void RowGroupCollection::Verify() {
#ifdef DEBUG
	idx_t current_total_rows = 0;
	row_groups->Verify();
	for (auto &row_group : row_groups->Segments()) {
		row_group.Verify();
		D_ASSERT(&row_group.GetCollection() == this);
		D_ASSERT(row_group.start == this->row_start + current_total_rows);
		current_total_rows += row_group.count;
	}
	D_ASSERT(current_total_rows == total_rows.load());
#endif
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void RowGroupCollection::InitializeScan(CollectionScanState &state, const vector<column_t> &column_ids,
                                        TableFilterSet *table_filters) {
	auto row_group = row_groups->GetRootSegment();
	D_ASSERT(row_group);
	state.row_groups = row_groups.get();
	state.max_row = row_start + total_rows;
	state.Initialize(GetTypes());
	while (row_group && !row_group->InitializeScan(state)) {
		row_group = row_groups->GetNextSegment(row_group);
	}
}

void RowGroupCollection::InitializeCreateIndexScan(CreateIndexScanState &state) {
	state.segment_lock = row_groups->Lock();
}

void RowGroupCollection::InitializeScanWithOffset(CollectionScanState &state, const vector<column_t> &column_ids,
                                                  idx_t start_row, idx_t end_row) {
	auto row_group = row_groups->GetSegment(start_row);
	D_ASSERT(row_group);
	state.row_groups = row_groups.get();
	state.max_row = end_row;
	state.Initialize(GetTypes());
	idx_t start_vector = (start_row - row_group->start) / STANDARD_VECTOR_SIZE;
	if (!row_group->InitializeScanWithOffset(state, start_vector)) {
		throw InternalException("Failed to initialize row group scan with offset");
	}
}

bool RowGroupCollection::InitializeScanInRowGroup(CollectionScanState &state, RowGroupCollection &collection,
                                                  RowGroup &row_group, idx_t vector_index, idx_t max_row) {
	state.max_row = max_row;
	state.row_groups = collection.row_groups.get();
	if (!state.column_scans) {
		// initialize the scan state
		state.Initialize(collection.GetTypes());
	}
	return row_group.InitializeScanWithOffset(state, vector_index);
}

void RowGroupCollection::InitializeParallelScan(ParallelCollectionScanState &state) {
	state.collection = this;
	state.current_row_group = row_groups->GetRootSegment();
	state.vector_index = 0;
	state.max_row = row_start + total_rows;
	state.batch_index = 0;
	state.processed_rows = 0;
}

bool RowGroupCollection::NextParallelScan(ClientContext &context, ParallelCollectionScanState &state,
                                          CollectionScanState &scan_state) {
	while (true) {
		idx_t vector_index;
		idx_t max_row;
		RowGroupCollection *collection;
		RowGroup *row_group;
		{
			// select the next row group to scan from the parallel state
			lock_guard<mutex> l(state.lock);
			if (!state.current_row_group || state.current_row_group->count == 0) {
				// no more data left to scan
				break;
			}
			collection = state.collection;
			row_group = state.current_row_group;
			if (ClientConfig::GetConfig(context).verify_parallelism) {
				vector_index = state.vector_index;
				max_row = state.current_row_group->start +
				          MinValue<idx_t>(state.current_row_group->count,
				                          STANDARD_VECTOR_SIZE * state.vector_index + STANDARD_VECTOR_SIZE);
				D_ASSERT(vector_index * STANDARD_VECTOR_SIZE < state.current_row_group->count);
				state.vector_index++;
				if (state.vector_index * STANDARD_VECTOR_SIZE >= state.current_row_group->count) {
					state.current_row_group = row_groups->GetNextSegment(state.current_row_group);
					state.vector_index = 0;
				}
			} else {
				state.processed_rows += state.current_row_group->count;
				vector_index = 0;
				max_row = state.current_row_group->start + state.current_row_group->count;
				state.current_row_group = row_groups->GetNextSegment(state.current_row_group);
			}
			max_row = MinValue<idx_t>(max_row, state.max_row);
			scan_state.batch_index = ++state.batch_index;
		}
		D_ASSERT(collection);
		D_ASSERT(row_group);

		// initialize the scan for this row group
		bool need_to_scan = InitializeScanInRowGroup(scan_state, *collection, *row_group, vector_index, max_row);
		if (!need_to_scan) {
			// skip this row group
			continue;
		}
		return true;
	}
	return false;
}

bool RowGroupCollection::Scan(DuckTransaction &transaction, const vector<column_t> &column_ids,
                              const std::function<bool(DataChunk &chunk)> &fun) {
	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		scan_types.push_back(types[column_ids[i]]);
	}
	DataChunk chunk;
	chunk.Initialize(GetAllocator(), scan_types);

	// initialize the scan
	TableScanState state;
	state.Initialize(column_ids, nullptr);
	InitializeScan(state.local_state, column_ids, nullptr);

	while (true) {
		chunk.Reset();
		state.local_state.Scan(transaction, chunk);
		if (chunk.size() == 0) {
			return true;
		}
		if (!fun(chunk)) {
			return false;
		}
	}
}

bool RowGroupCollection::Scan(DuckTransaction &transaction, const std::function<bool(DataChunk &chunk)> &fun) {
	vector<column_t> column_ids;
	column_ids.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.push_back(i);
	}
	return Scan(transaction, column_ids, fun);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void RowGroupCollection::Fetch(TransactionData transaction, DataChunk &result, const vector<column_t> &column_ids,
                               const Vector &row_identifiers, idx_t fetch_count, ColumnFetchState &state) {
	// figure out which row_group to fetch from
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);
	idx_t count = 0;
	for (idx_t i = 0; i < fetch_count; i++) {
		auto row_id = row_ids[i];
		RowGroup *row_group;
		{
			idx_t segment_index;
			auto l = row_groups->Lock();
			if (!row_groups->TryGetSegmentIndex(l, row_id, segment_index)) {
				// in parallel append scenarios it is possible for the row_id
				continue;
			}
			row_group = row_groups->GetSegmentByIndex(l, segment_index);
		}
		if (!row_group->Fetch(transaction, row_id - row_group->start)) {
			continue;
		}
		row_group->FetchRow(transaction, state, column_ids, row_id, result, count);
		count++;
	}
	result.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
TableAppendState::TableAppendState()
    : row_group_append_state(*this), total_append_count(0), start_row_group(nullptr), transaction(0, 0), remaining(0) {
}

TableAppendState::~TableAppendState() {
	D_ASSERT(Exception::UncaughtException() || remaining == 0);
}

bool RowGroupCollection::IsEmpty() const {
	auto l = row_groups->Lock();
	return IsEmpty(l);
}

bool RowGroupCollection::IsEmpty(SegmentLock &l) const {
	return row_groups->IsEmpty(l);
}

void RowGroupCollection::InitializeAppend(TransactionData transaction, TableAppendState &state, idx_t append_count) {
	state.row_start = total_rows;
	state.current_row = state.row_start;
	state.total_append_count = 0;

	// start writing to the row_groups
	auto l = row_groups->Lock();
	if (IsEmpty(l)) {
		// empty row group collection: empty first row group
		AppendRowGroup(l, row_start);
	}
	state.start_row_group = row_groups->GetLastSegment(l);
	D_ASSERT(this->row_start + total_rows == state.start_row_group->start + state.start_row_group->count);
	state.start_row_group->InitializeAppend(state.row_group_append_state);
	state.remaining = append_count;
	state.transaction = transaction;
	if (state.remaining > 0) {
		state.start_row_group->AppendVersionInfo(transaction, state.remaining);
		total_rows += state.remaining;
	}
}

void RowGroupCollection::InitializeAppend(TableAppendState &state) {
	TransactionData tdata(0, 0);
	InitializeAppend(tdata, state, 0);
}

bool RowGroupCollection::Append(DataChunk &chunk, TableAppendState &state) {
	D_ASSERT(chunk.ColumnCount() == types.size());
	chunk.Verify();

	bool new_row_group = false;
	idx_t append_count = chunk.size();
	idx_t remaining = chunk.size();
	state.total_append_count += append_count;
	while (true) {
		auto current_row_group = state.row_group_append_state.row_group;
		// check how much we can fit into the current row_group
		idx_t append_count =
		    MinValue<idx_t>(remaining, Storage::ROW_GROUP_SIZE - state.row_group_append_state.offset_in_row_group);
		if (append_count > 0) {
			current_row_group->Append(state.row_group_append_state, chunk, append_count);
			// merge the stats
			auto stats_lock = stats.GetLock();
			for (idx_t i = 0; i < types.size(); i++) {
				current_row_group->MergeIntoStatistics(i, stats.GetStats(i).Statistics());
			}
		}
		remaining -= append_count;
		if (state.remaining > 0) {
			state.remaining -= append_count;
		}
		if (remaining > 0) {
			// we expect max 1 iteration of this loop (i.e. a single chunk should never overflow more than one
			// row_group)
			D_ASSERT(chunk.size() == remaining + append_count);
			// slice the input chunk
			if (remaining < chunk.size()) {
				SelectionVector sel(remaining);
				for (idx_t i = 0; i < remaining; i++) {
					sel.set_index(i, append_count + i);
				}
				chunk.Slice(sel, remaining);
			}
			// append a new row_group
			new_row_group = true;
			auto next_start = current_row_group->start + state.row_group_append_state.offset_in_row_group;

			auto l = row_groups->Lock();
			AppendRowGroup(l, next_start);
			// set up the append state for this row_group
			auto last_row_group = row_groups->GetLastSegment(l);
			last_row_group->InitializeAppend(state.row_group_append_state);
			if (state.remaining > 0) {
				last_row_group->AppendVersionInfo(state.transaction, state.remaining);
			}
			continue;
		} else {
			break;
		}
	}
	state.current_row += append_count;
	auto stats_lock = stats.GetLock();
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		stats.GetStats(col_idx).UpdateDistinctStatistics(chunk.data[col_idx], chunk.size());
	}
	return new_row_group;
}

void RowGroupCollection::FinalizeAppend(TransactionData transaction, TableAppendState &state) {
	auto remaining = state.total_append_count;
	auto row_group = state.start_row_group;
	while (remaining > 0) {
		auto append_count = MinValue<idx_t>(remaining, Storage::ROW_GROUP_SIZE - row_group->count);
		row_group->AppendVersionInfo(transaction, append_count);
		remaining -= append_count;
		row_group = row_groups->GetNextSegment(row_group);
	}
	total_rows += state.total_append_count;

	state.total_append_count = 0;
	state.start_row_group = nullptr;

	Verify();
}

void RowGroupCollection::CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count) {
	auto row_group = row_groups->GetSegment(row_start);
	D_ASSERT(row_group);
	idx_t current_row = row_start;
	idx_t remaining = count;
	while (true) {
		idx_t start_in_row_group = current_row - row_group->start;
		idx_t append_count = MinValue<idx_t>(row_group->count - start_in_row_group, remaining);

		row_group->CommitAppend(commit_id, start_in_row_group, append_count);

		current_row += append_count;
		remaining -= append_count;
		if (remaining == 0) {
			break;
		}
		row_group = row_groups->GetNextSegment(row_group);
	}
}

void RowGroupCollection::RevertAppendInternal(idx_t start_row, idx_t count) {
	if (total_rows != start_row + count) {
		throw InternalException("Interleaved appends: this should no longer happen");
	}
	total_rows = start_row;

	auto l = row_groups->Lock();
	// find the segment index that the current row belongs to
	idx_t segment_index = row_groups->GetSegmentIndex(l, start_row);
	auto segment = row_groups->GetSegmentByIndex(l, segment_index);
	auto &info = *segment;

	// remove any segments AFTER this segment: they should be deleted entirely
	row_groups->EraseSegments(l, segment_index);

	info.next = nullptr;
	info.RevertAppend(start_row);
}

void RowGroupCollection::MergeStorage(RowGroupCollection &data) {
	D_ASSERT(data.types == types);
	auto index = row_start + total_rows.load();
	auto segments = data.row_groups->MoveSegments();
	for (auto &entry : segments) {
		auto &row_group = entry.node;
		row_group->MoveToCollection(*this, index);
		index += row_group->count;
		row_groups->AppendSegment(std::move(row_group));
	}
	stats.MergeStats(data.stats);
	total_rows += data.total_rows.load();
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
idx_t RowGroupCollection::Delete(TransactionData transaction, DataTable &table, row_t *ids, idx_t count) {
	idx_t delete_count = 0;
	// delete is in the row groups
	// we need to figure out for each id to which row group it belongs
	// usually all (or many) ids belong to the same row group
	// we iterate over the ids and check for every id if it belongs to the same row group as their predecessor
	idx_t pos = 0;
	do {
		idx_t start = pos;
		auto row_group = row_groups->GetSegment(ids[start]);
		for (pos++; pos < count; pos++) {
			D_ASSERT(ids[pos] >= 0);
			// check if this id still belongs to this row group
			if (idx_t(ids[pos]) < row_group->start) {
				// id is before row_group start -> it does not
				break;
			}
			if (idx_t(ids[pos]) >= row_group->start + row_group->count) {
				// id is after row group end -> it does not
				break;
			}
		}
		delete_count += row_group->Delete(transaction, table, ids + start, pos - start);
	} while (pos < count);
	return delete_count;
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
void RowGroupCollection::Update(TransactionData transaction, row_t *ids, const vector<PhysicalIndex> &column_ids,
                                DataChunk &updates) {
	idx_t pos = 0;
	do {
		idx_t start = pos;
		auto row_group = row_groups->GetSegment(ids[pos]);
		row_t base_id =
		    row_group->start + ((ids[pos] - row_group->start) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE);
		row_t max_id = MinValue<row_t>(base_id + STANDARD_VECTOR_SIZE, row_group->start + row_group->count);
		for (pos++; pos < updates.size(); pos++) {
			D_ASSERT(ids[pos] >= 0);
			// check if this id still belongs to this vector in this row group
			if (ids[pos] < base_id) {
				// id is before vector start -> it does not
				break;
			}
			if (ids[pos] >= max_id) {
				// id is after the maximum id in this vector -> it does not
				break;
			}
		}
		row_group->Update(transaction, updates, ids, start, pos - start, column_ids);

		auto l = stats.GetLock();
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto column_id = column_ids[i];
			stats.MergeStats(*l, column_id.index, *row_group->GetStatistics(column_id.index));
		}
	} while (pos < updates.size());
}

void RowGroupCollection::RemoveFromIndexes(TableIndexList &indexes, Vector &row_identifiers, idx_t count) {
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);

	// initialize the fetch state
	// FIXME: we do not need to fetch all columns, only the columns required by the indices!
	TableScanState state;
	vector<column_t> column_ids;
	column_ids.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.push_back(i);
	}
	state.Initialize(std::move(column_ids));
	state.table_state.max_row = row_start + total_rows;

	// initialize the fetch chunk
	DataChunk result;
	result.Initialize(GetAllocator(), types);

	SelectionVector sel(STANDARD_VECTOR_SIZE);
	// now iterate over the row ids
	for (idx_t r = 0; r < count;) {
		result.Reset();
		// figure out which row_group to fetch from
		auto row_id = row_ids[r];
		auto row_group = row_groups->GetSegment(row_id);
		auto row_group_vector_idx = (row_id - row_group->start) / STANDARD_VECTOR_SIZE;
		auto base_row_id = row_group_vector_idx * STANDARD_VECTOR_SIZE + row_group->start;

		// fetch the current vector
		state.table_state.Initialize(GetTypes());
		row_group->InitializeScanWithOffset(state.table_state, row_group_vector_idx);
		row_group->ScanCommitted(state.table_state, result, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		result.Verify();

		// check for any remaining row ids if they also fall into this vector
		// we try to fetch handle as many rows as possible at the same time
		idx_t sel_count = 0;
		for (; r < count; r++) {
			idx_t current_row = idx_t(row_ids[r]);
			if (current_row < base_row_id || current_row >= base_row_id + result.size()) {
				// this row-id does not fall into the current chunk - break
				break;
			}
			auto row_in_vector = current_row - base_row_id;
			D_ASSERT(row_in_vector < result.size());
			sel.set_index(sel_count++, row_in_vector);
		}
		D_ASSERT(sel_count > 0);
		// slice the vector with all rows that are present in this vector and erase from the index
		result.Slice(sel, sel_count);

		indexes.Scan([&](Index &index) {
			index.Delete(result, row_identifiers);
			return false;
		});
	}
}

void RowGroupCollection::UpdateColumn(TransactionData transaction, Vector &row_ids, const vector<column_t> &column_path,
                                      DataChunk &updates) {
	auto first_id = FlatVector::GetValue<row_t>(row_ids, 0);
	if (first_id >= MAX_ROW_ID) {
		throw NotImplementedException("Cannot update a column-path on transaction local data");
	}
	// find the row_group this id belongs to
	auto primary_column_idx = column_path[0];
	auto row_group = row_groups->GetSegment(first_id);
	row_group->UpdateColumn(transaction, updates, row_ids, column_path);

	row_group->MergeIntoStatistics(primary_column_idx, stats.GetStats(primary_column_idx).Statistics());
}

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
void RowGroupCollection::Checkpoint(TableDataWriter &writer, TableStatistics &global_stats) {
	bool can_vacuum_deletes = info->indexes.Empty();
	idx_t start = this->row_start;
	auto segments = row_groups->MoveSegments();
	auto l = row_groups->Lock();
	for (auto &entry : segments) {
		auto &row_group = *entry.node;
		if (can_vacuum_deletes && row_group.AllDeleted()) {
			row_group.CommitDrop();
			continue;
		}
		row_group.MoveToCollection(*this, start);
		auto row_group_writer = writer.GetRowGroupWriter(row_group);
		auto pointer = row_group.Checkpoint(*row_group_writer, global_stats);
		writer.AddRowGroup(std::move(pointer), std::move(row_group_writer));
		row_groups->AppendSegment(l, std::move(entry.node));
		start += row_group.count;
	}
	total_rows = start;
}

//===--------------------------------------------------------------------===//
// CommitDrop
//===--------------------------------------------------------------------===//
void RowGroupCollection::CommitDropColumn(idx_t index) {
	for (auto &row_group : row_groups->Segments()) {
		row_group.CommitDropColumn(index);
	}
}

void RowGroupCollection::CommitDropTable() {
	for (auto &row_group : row_groups->Segments()) {
		row_group.CommitDrop();
	}
}

//===--------------------------------------------------------------------===//
// GetColumnSegmentInfo
//===--------------------------------------------------------------------===//
vector<ColumnSegmentInfo> RowGroupCollection::GetColumnSegmentInfo() {
	vector<ColumnSegmentInfo> result;
	for (auto &row_group : row_groups->Segments()) {
		row_group.GetColumnSegmentInfo(row_group.index, result);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Alter
//===--------------------------------------------------------------------===//
shared_ptr<RowGroupCollection> RowGroupCollection::AddColumn(ClientContext &context, ColumnDefinition &new_column,
                                                             Expression &default_value) {
	idx_t new_column_idx = types.size();
	auto new_types = types;
	new_types.push_back(new_column.GetType());
	auto result =
	    make_shared<RowGroupCollection>(info, block_manager, std::move(new_types), row_start, total_rows.load());

	ExpressionExecutor executor(context);
	DataChunk dummy_chunk;
	Vector default_vector(new_column.GetType());
	executor.AddExpression(default_value);

	result->stats.InitializeAddColumn(stats, new_column.GetType());
	auto &new_column_stats = result->stats.GetStats(new_column_idx);

	// fill the column with its DEFAULT value, or NULL if none is specified
	auto new_stats = make_uniq<SegmentStatistics>(new_column.GetType());
	for (auto &current_row_group : row_groups->Segments()) {
		auto new_row_group = current_row_group.AddColumn(*result, new_column, executor, default_value, default_vector);
		// merge in the statistics
		new_row_group->MergeIntoStatistics(new_column_idx, new_column_stats.Statistics());

		result->row_groups->AppendSegment(std::move(new_row_group));
	}
	return result;
}

shared_ptr<RowGroupCollection> RowGroupCollection::RemoveColumn(idx_t col_idx) {
	D_ASSERT(col_idx < types.size());
	auto new_types = types;
	new_types.erase(new_types.begin() + col_idx);

	auto result =
	    make_shared<RowGroupCollection>(info, block_manager, std::move(new_types), row_start, total_rows.load());
	result->stats.InitializeRemoveColumn(stats, col_idx);

	for (auto &current_row_group : row_groups->Segments()) {
		auto new_row_group = current_row_group.RemoveColumn(*result, col_idx);
		result->row_groups->AppendSegment(std::move(new_row_group));
	}
	return result;
}

shared_ptr<RowGroupCollection> RowGroupCollection::AlterType(ClientContext &context, idx_t changed_idx,
                                                             const LogicalType &target_type,
                                                             vector<column_t> bound_columns, Expression &cast_expr) {
	D_ASSERT(changed_idx < types.size());
	auto new_types = types;
	new_types[changed_idx] = target_type;

	auto result =
	    make_shared<RowGroupCollection>(info, block_manager, std::move(new_types), row_start, total_rows.load());
	result->stats.InitializeAlterType(stats, changed_idx, target_type);

	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < bound_columns.size(); i++) {
		if (bound_columns[i] == COLUMN_IDENTIFIER_ROW_ID) {
			scan_types.emplace_back(LogicalType::ROW_TYPE);
		} else {
			scan_types.push_back(types[bound_columns[i]]);
		}
	}
	DataChunk scan_chunk;
	scan_chunk.Initialize(GetAllocator(), scan_types);

	ExpressionExecutor executor(context);
	executor.AddExpression(cast_expr);

	TableScanState scan_state;
	scan_state.Initialize(bound_columns);
	scan_state.table_state.max_row = row_start + total_rows;

	// now alter the type of the column within all of the row_groups individually
	auto &changed_stats = result->stats.GetStats(changed_idx);
	for (auto &current_row_group : row_groups->Segments()) {
		auto new_row_group = current_row_group.AlterType(*result, target_type, changed_idx, executor,
		                                                 scan_state.table_state, scan_chunk);
		new_row_group->MergeIntoStatistics(changed_idx, changed_stats.Statistics());
		result->row_groups->AppendSegment(std::move(new_row_group));
	}

	return result;
}

void RowGroupCollection::VerifyNewConstraint(DataTable &parent, const BoundConstraint &constraint) {
	if (total_rows == 0) {
		return;
	}
	// scan the original table, check if there's any null value
	auto &not_null_constraint = constraint.Cast<BoundNotNullConstraint>();
	vector<LogicalType> scan_types;
	auto physical_index = not_null_constraint.index.index;
	D_ASSERT(physical_index < types.size());
	scan_types.push_back(types[physical_index]);
	DataChunk scan_chunk;
	scan_chunk.Initialize(GetAllocator(), scan_types);

	CreateIndexScanState state;
	vector<column_t> cids;
	cids.push_back(physical_index);
	// Use ScanCommitted to scan the latest committed data
	state.Initialize(cids, nullptr);
	InitializeScan(state.table_state, cids, nullptr);
	InitializeCreateIndexScan(state);
	while (true) {
		scan_chunk.Reset();
		state.table_state.ScanCommitted(scan_chunk, state.segment_lock,
		                                TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED);
		if (scan_chunk.size() == 0) {
			break;
		}
		// Check constraint
		if (VectorOperations::HasNull(scan_chunk.data[0], scan_chunk.size())) {
			throw ConstraintException("NOT NULL constraint failed: %s.%s", info->table,
			                          parent.column_definitions[physical_index].GetName());
		}
	}
}

//===--------------------------------------------------------------------===//
// Statistics
//===--------------------------------------------------------------------===//
void RowGroupCollection::CopyStats(TableStatistics &other_stats) {
	stats.CopyStats(other_stats);
}

unique_ptr<BaseStatistics> RowGroupCollection::CopyStats(column_t column_id) {
	return stats.CopyStats(column_id);
}

void RowGroupCollection::SetDistinct(column_t column_id, unique_ptr<DistinctStatistics> distinct_stats) {
	D_ASSERT(column_id != COLUMN_IDENTIFIER_ROW_ID);
	auto stats_guard = stats.GetLock();
	stats.GetStats(column_id).SetDistinct(std::move(distinct_stats));
}

} // namespace duckdb
