#include "duckdb/storage/table/row_group_collection.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/execution/task_error_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Row Group Segment Tree
//===--------------------------------------------------------------------===//
RowGroupSegmentTree::RowGroupSegmentTree(RowGroupCollection &collection, idx_t base_row_id)
    : SegmentTree<RowGroup, true>(base_row_id), collection(collection), current_row_group(0), max_row_group(0) {
}
RowGroupSegmentTree::~RowGroupSegmentTree() {
}

void RowGroupSegmentTree::Initialize(PersistentTableData &data) {
	D_ASSERT(data.row_group_count > 0);
	current_row_group = 0;
	max_row_group = data.row_group_count;
	finished_loading = false;
	reader = make_uniq<MetadataReader>(collection.GetMetadataManager(), data.block_pointer);
	root_pointer = data.block_pointer;
}

shared_ptr<RowGroup> RowGroupSegmentTree::LoadSegment() const {
	if (current_row_group >= max_row_group) {
		reader.reset();
		finished_loading = true;
		return nullptr;
	}
	BinaryDeserializer deserializer(*reader);
	deserializer.Begin();
	auto row_group_pointer = RowGroup::Deserialize(deserializer);
	deserializer.End();
	current_row_group++;
	return make_shared_ptr<RowGroup>(collection, std::move(row_group_pointer));
}

//===--------------------------------------------------------------------===//
// Row Group Collection
//===--------------------------------------------------------------------===//
RowGroupCollection::RowGroupCollection(shared_ptr<DataTableInfo> info_p, TableIOManager &io_manager,
                                       vector<LogicalType> types_p, idx_t row_start, idx_t total_rows)
    : RowGroupCollection(std::move(info_p), io_manager.GetBlockManagerForRowData(), std::move(types_p), row_start,
                         total_rows, io_manager.GetRowGroupSize()) {
}

RowGroupCollection::RowGroupCollection(shared_ptr<DataTableInfo> info_p, BlockManager &block_manager,
                                       vector<LogicalType> types_p, idx_t row_start, idx_t total_rows_p,
                                       idx_t row_group_size_p)
    : block_manager(block_manager), row_group_size(row_group_size_p), total_rows(total_rows_p), info(std::move(info_p)),
      types(std::move(types_p)), owned_row_groups(make_shared_ptr<RowGroupSegmentTree>(*this, row_start)),
      allocation_size(0), requires_new_row_group(false) {
}

idx_t RowGroupCollection::GetTotalRows() const {
	return total_rows.load();
}

const vector<LogicalType> &RowGroupCollection::GetTypes() const {
	return types;
}

Allocator &RowGroupCollection::GetAllocator() const {
	return Allocator::Get(info->GetDB());
}

AttachedDatabase &RowGroupCollection::GetAttached() {
	return GetTableInfo().GetDB();
}

MetadataManager &RowGroupCollection::GetMetadataManager() {
	return GetBlockManager().GetMetadataManager();
}

idx_t RowGroupCollection::GetBaseRowId() const {
	return owned_row_groups->GetBaseRowId();
}

shared_ptr<RowGroupSegmentTree> RowGroupCollection::GetRowGroups() const {
	lock_guard<mutex> guard(row_group_pointer_lock);
	return owned_row_groups;
}

void RowGroupCollection::SetRowGroups(shared_ptr<RowGroupSegmentTree> new_row_groups) {
	lock_guard<mutex> guard(row_group_pointer_lock);
	owned_row_groups = std::move(new_row_groups);
}

//===--------------------------------------------------------------------===//
// Initialize
//===--------------------------------------------------------------------===//
void RowGroupCollection::Initialize(PersistentTableData &data) {
	D_ASSERT(this->GetBaseRowId() == 0);
	auto l = owned_row_groups->Lock();
	this->total_rows = data.total_rows;
	owned_row_groups->Initialize(data);
	stats.Initialize(types, data);
	metadata_pointer = data.base_table_pointer;
}

void RowGroupCollection::FinalizeCheckpoint(MetaBlockPointer pointer) {
	metadata_pointer = pointer;
}

void RowGroupCollection::Initialize(PersistentCollectionData &data) {
	stats.InitializeEmpty(types);
	auto l = owned_row_groups->Lock();
	for (auto &row_group_data : data.row_group_data) {
		auto row_group = make_uniq<RowGroup>(*this, row_group_data);
		row_group->MergeIntoStatistics(stats);
		total_rows += row_group->count;
		owned_row_groups->AppendSegment(l, std::move(row_group), row_group_data.start);
	}
}

void RowGroupCollection::SetAppendRequiresNewRowGroup() {
	requires_new_row_group = true;
}

void RowGroupCollection::InitializeEmpty() {
	stats.InitializeEmpty(types);
}

ColumnDataType GetColumnDataType(idx_t row_start) {
	if (row_start == UnsafeNumericCast<idx_t>(MAX_ROW_ID)) {
		return ColumnDataType::INITIAL_TRANSACTION_LOCAL;
	}
	if (row_start > UnsafeNumericCast<idx_t>(MAX_ROW_ID)) {
		return ColumnDataType::TRANSACTION_LOCAL;
	}
	return ColumnDataType::MAIN_TABLE;
}

void RowGroupCollection::AppendRowGroup(SegmentLock &l, idx_t start_row) {
	D_ASSERT(start_row >= GetBaseRowId());
	auto new_row_group = make_uniq<RowGroup>(*this, 0U);
	new_row_group->InitializeEmpty(types, GetColumnDataType(start_row));
	owned_row_groups->AppendSegment(l, std::move(new_row_group), start_row);
	requires_new_row_group = false;
}

optional_ptr<RowGroup> RowGroupCollection::GetRowGroup(int64_t index) {
	auto result = owned_row_groups->GetSegmentByIndex(index);
	if (!result) {
		return nullptr;
	}
	return result->node.get();
}

void RowGroupCollection::SetRowGroup(int64_t index, shared_ptr<RowGroup> new_row_group) {
	auto result = owned_row_groups->GetSegmentByIndex(index);
	if (!result) {
		throw InternalException("RowGroupCollection::SetRowGroup - Segment is out of range");
	}
	result->node = std::move(new_row_group);
}

void RowGroupCollection::Verify() {
#ifdef DEBUG
	idx_t current_total_rows = 0;
	auto row_groups = GetRowGroups();
	row_groups->Verify();
	for (auto &entry : row_groups->SegmentNodes()) {
		auto &row_group = *entry.node;
		row_group.Verify();
		D_ASSERT(&row_group.GetCollection() == this);
		D_ASSERT(entry.row_start == this->GetBaseRowId() + current_total_rows);
		current_total_rows += row_group.count;
	}
	D_ASSERT(current_total_rows == total_rows.load());
#endif
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void RowGroupCollection::InitializeScan(const QueryContext &context, CollectionScanState &state,
                                        const vector<StorageIndex> &column_ids,
                                        optional_ptr<TableFilterSet> table_filters) {
	state.row_groups = GetRowGroups();
	auto row_group = state.GetRootSegment();
	D_ASSERT(row_group);
	state.max_row = GetBaseRowId() + total_rows;
	state.Initialize(context, GetTypes());
	while (row_group && !row_group->node->InitializeScan(state, *row_group)) {
		row_group = state.GetNextRowGroup(*row_group);
	}
}

void RowGroupCollection::InitializeCreateIndexScan(CreateIndexScanState &state) {
	state.row_groups = GetRowGroups();
	state.segment_lock = state.row_groups->Lock();
}

void RowGroupCollection::InitializeScanWithOffset(const QueryContext &context, CollectionScanState &state,
                                                  const vector<StorageIndex> &column_ids, idx_t start_row,
                                                  idx_t end_row) {
	state.row_groups = GetRowGroups();
	auto row_group = state.row_groups->GetSegment(start_row);
	D_ASSERT(row_group);
	state.max_row = end_row;
	state.Initialize(context, GetTypes());
	idx_t start_vector = (start_row - row_group->row_start) / STANDARD_VECTOR_SIZE;
	if (!row_group->node->InitializeScanWithOffset(state, *row_group, start_vector)) {
		throw InternalException("Failed to initialize row group scan with offset");
	}
}

bool RowGroupCollection::InitializeScanInRowGroup(const QueryContext &context, CollectionScanState &state,
                                                  RowGroupCollection &collection, SegmentNode<RowGroup> &row_group,
                                                  idx_t vector_index, idx_t max_row) {
	state.max_row = max_row;
	state.row_groups = collection.GetRowGroups();
	if (state.column_scans.empty()) {
		// initialize the scan state
		state.Initialize(context, collection.GetTypes());
	}
	return row_group.node->InitializeScanWithOffset(state, row_group, vector_index);
}

void RowGroupCollection::InitializeParallelScan(ParallelCollectionScanState &state) {
	state.collection = this;
	state.row_groups = GetRowGroups();
	state.current_row_group = state.GetRootSegment(*state.row_groups);
	state.vector_index = 0;
	state.max_row = GetBaseRowId() + total_rows;
	state.batch_index = 0;
	state.processed_rows = 0;
}

bool RowGroupCollection::NextParallelScan(ClientContext &context, ParallelCollectionScanState &state,
                                          CollectionScanState &scan_state) {
	AssignSharedPointer(scan_state.row_groups, state.row_groups);
	while (true) {
		idx_t vector_index;
		idx_t max_row;
		RowGroupCollection *collection;
		optional_ptr<SegmentNode<RowGroup>> row_group;
		{
			// select the next row group to scan from the parallel state
			lock_guard<mutex> l(state.lock);
			if (!state.current_row_group) {
				// no more data left to scan
				break;
			}
			auto &current_row_group = *state.current_row_group->node;
			if (current_row_group.count == 0) {
				break;
			}
			auto row_start = state.current_row_group->row_start;
			collection = state.collection;
			row_group = state.current_row_group;
			if (ClientConfig::GetConfig(context).verify_parallelism) {
				vector_index = state.vector_index;
				max_row = row_start + MinValue<idx_t>(current_row_group.count,
				                                      STANDARD_VECTOR_SIZE * state.vector_index + STANDARD_VECTOR_SIZE);
				D_ASSERT(vector_index * STANDARD_VECTOR_SIZE < current_row_group.count);
				state.vector_index++;
				if (state.vector_index * STANDARD_VECTOR_SIZE >= current_row_group.count) {
					state.current_row_group = state.GetNextRowGroup(*state.row_groups, *row_group).get();
					state.vector_index = 0;
				}
			} else {
				state.processed_rows += current_row_group.count;
				vector_index = 0;
				max_row = row_start + current_row_group.count;
				state.current_row_group = state.GetNextRowGroup(*state.row_groups, *row_group).get();
			}
			max_row = MinValue<idx_t>(max_row, state.max_row);
			scan_state.batch_index = ++state.batch_index;
		}
		D_ASSERT(collection);
		D_ASSERT(row_group);

		// initialize the scan for this row group
		bool need_to_scan =
		    InitializeScanInRowGroup(context, scan_state, *collection, *row_group, vector_index, max_row);
		if (!need_to_scan) {
			// skip this row group
			continue;
		}
		return true;
	}
	lock_guard<mutex> l(state.lock);
	scan_state.batch_index = state.batch_index;
	return false;
}

bool RowGroupCollection::Scan(DuckTransaction &transaction, const vector<StorageIndex> &column_ids,
                              const std::function<bool(DataChunk &chunk)> &fun) {
	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		scan_types.push_back(types[column_ids[i].GetPrimaryIndex()]);
	}
	DataChunk chunk;
	chunk.Initialize(GetAllocator(), scan_types);

	// initialize the scan
	TableScanState state;
	state.Initialize(column_ids, nullptr);
	InitializeScan(QueryContext(), state.local_state, column_ids, nullptr);

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
	vector<StorageIndex> column_ids;
	column_ids.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.emplace_back(i);
	}
	return Scan(transaction, column_ids, fun);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void RowGroupCollection::Fetch(TransactionData transaction, DataChunk &result, const vector<StorageIndex> &column_ids,
                               const Vector &row_identifiers, idx_t fetch_count, ColumnFetchState &state) {
	// figure out which row_group to fetch from
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);
	idx_t count = 0;
	auto row_groups = GetRowGroups();
	for (idx_t i = 0; i < fetch_count; i++) {
		auto row_id = row_ids[i];
		optional_ptr<SegmentNode<RowGroup>> row_group;
		{
			idx_t segment_index;
			auto l = row_groups->Lock();
			if (!row_groups->TryGetSegmentIndex(l, UnsafeNumericCast<idx_t>(row_id), segment_index)) {
				// in parallel append scenarios it is possible for the row_id
				continue;
			}
			row_group = row_groups->GetSegmentByIndex(l, UnsafeNumericCast<int64_t>(segment_index));
		}
		auto &current_row_group = *row_group->node;
		auto offset_in_row_group = UnsafeNumericCast<idx_t>(row_id) - row_group->row_start;
		if (!current_row_group.Fetch(transaction, offset_in_row_group)) {
			continue;
		}
		state.row_group = row_group;
		current_row_group.FetchRow(transaction, state, column_ids, UnsafeNumericCast<row_t>(offset_in_row_group),
		                           result, count);
		count++;
	}
	result.SetCardinality(count);
}

bool RowGroupCollection::CanFetch(TransactionData transaction, const row_t row_id) {
	auto row_groups = GetRowGroups();
	optional_ptr<SegmentNode<RowGroup>> row_group;
	{
		idx_t segment_index;
		auto l = row_groups->Lock();
		if (!row_groups->TryGetSegmentIndex(l, UnsafeNumericCast<idx_t>(row_id), segment_index)) {
			return false;
		}
		row_group = row_groups->GetSegmentByIndex(l, UnsafeNumericCast<int64_t>(segment_index));
	}
	auto &current_row_group = *row_group->node;
	auto offset_in_row_group = UnsafeNumericCast<idx_t>(row_id) - row_group->row_start;
	return current_row_group.Fetch(transaction, offset_in_row_group);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
TableAppendState::TableAppendState()
    : row_group_append_state(*this), total_append_count(0), start_row_group(nullptr), transaction(0, 0),
      hashes(LogicalType::HASH) {
}

TableAppendState::~TableAppendState() {
}

bool RowGroupCollection::IsEmpty() const {
	auto row_groups = GetRowGroups();
	auto l = row_groups->Lock();
	return row_groups->IsEmpty(l);
}

void RowGroupCollection::InitializeAppend(TransactionData transaction, TableAppendState &state) {
	state.row_start = UnsafeNumericCast<row_t>(total_rows.load());
	state.current_row = state.row_start;
	state.total_append_count = 0;

	// start writing to the row_groups
	state.row_groups = GetRowGroups();
	auto l = state.row_groups->Lock();
	if (state.row_groups->IsEmpty(l) || requires_new_row_group) {
		// empty row group collection: empty first row group
		AppendRowGroup(l, GetBaseRowId() + total_rows);
	}
	state.start_row_group = state.row_groups->GetLastSegment(l);
	D_ASSERT(GetBaseRowId() + total_rows == state.start_row_group->row_start + state.start_row_group->node->count);
	state.start_row_group->node->InitializeAppend(state.row_group_append_state);
	state.transaction = transaction;
	state.row_group_start = state.start_row_group->row_start;

	// initialize thread-local stats so we have less lock contention when updating distinct statistics
	state.stats = TableStatistics();
	state.stats.InitializeEmpty(types);
}

void RowGroupCollection::InitializeAppend(TableAppendState &state) {
	TransactionData tdata(0, 0);
	InitializeAppend(tdata, state);
}

bool RowGroupCollection::Append(DataChunk &chunk, TableAppendState &state) {
	const idx_t row_group_size = GetRowGroupSize();
	D_ASSERT(chunk.ColumnCount() == types.size());
	chunk.Verify();

	bool new_row_group = false;
	idx_t total_append_count = chunk.size();
	idx_t remaining = chunk.size();
	state.total_append_count += total_append_count;
	while (true) {
		auto current_row_group = state.row_group_append_state.row_group;
		// check how much we can fit into the current row_group
		idx_t append_count =
		    MinValue<idx_t>(remaining, row_group_size - state.row_group_append_state.offset_in_row_group);
		if (append_count > 0) {
			auto previous_allocation_size = current_row_group->GetAllocationSize();
			current_row_group->Append(state.row_group_append_state, chunk, append_count);
			allocation_size += current_row_group->GetAllocationSize() - previous_allocation_size;
			// merge the stats
			current_row_group->MergeIntoStatistics(stats);
		}
		remaining -= append_count;
		if (remaining == 0) {
			break;
		}
		// we expect max 1 iteration of this loop (i.e. a single chunk should never overflow more than one
		// row_group)
		D_ASSERT(chunk.size() == remaining + append_count);
		// slice the input chunk
		if (remaining < chunk.size()) {
			chunk.Slice(append_count, remaining);
		}
		// append a new row_group
		new_row_group = true;
		auto next_start = state.row_group_start + state.row_group_append_state.offset_in_row_group;

		auto l = state.row_groups->Lock();
		AppendRowGroup(l, next_start);
		// set up the append state for this row_group
		auto last_row_group = state.row_groups->GetLastSegment(l);
		last_row_group->node->InitializeAppend(state.row_group_append_state);
		state.row_group_start = next_start;
	}
	state.current_row += row_t(total_append_count);

	auto local_stats_lock = state.stats.GetLock();

	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		auto &column_stats = state.stats.GetStats(*local_stats_lock, col_idx);
		column_stats.UpdateDistinctStatistics(chunk.data[col_idx], chunk.size(), state.hashes);
	}

	return new_row_group;
}

void RowGroupCollection::FinalizeAppend(TransactionData transaction, TableAppendState &state) {
	const idx_t row_group_size = GetRowGroupSize();

	auto remaining = state.total_append_count;
	auto row_group = state.start_row_group;
	while (remaining > 0) {
		auto &current_row_group = *row_group->node;
		auto append_count = MinValue<idx_t>(remaining, row_group_size - current_row_group.count);
		current_row_group.AppendVersionInfo(transaction, append_count);
		remaining -= append_count;
		row_group = state.row_groups->GetNextSegment(*row_group);
	}
	total_rows += state.total_append_count;

	state.total_append_count = 0;
	state.start_row_group = nullptr;

	auto local_stats_lock = state.stats.GetLock();
	auto global_stats_lock = stats.GetLock();
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		auto &global_stats = stats.GetStats(*global_stats_lock, col_idx);
		if (!global_stats.HasDistinctStats()) {
			continue;
		}
		auto &local_stats = state.stats.GetStats(*local_stats_lock, col_idx);
		if (!local_stats.HasDistinctStats()) {
			continue;
		}
		global_stats.DistinctStats().Merge(local_stats.DistinctStats());
	}

	Verify();
}

void RowGroupCollection::CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count) {
	auto row_groups = GetRowGroups();
	auto row_group = row_groups->GetSegment(row_start);
	D_ASSERT(row_group);
	idx_t current_row = row_start;
	idx_t remaining = count;
	while (true) {
		auto &current_row_group = *row_group->node;
		idx_t start_in_row_group = current_row - row_group->row_start;
		idx_t append_count = MinValue<idx_t>(current_row_group.count - start_in_row_group, remaining);

		current_row_group.CommitAppend(commit_id, start_in_row_group, append_count);

		current_row += append_count;
		remaining -= append_count;
		if (remaining == 0) {
			break;
		}
		row_group = row_groups->GetNextSegment(*row_group);
	}
}

void RowGroupCollection::RevertAppendInternal(idx_t start_row) {
	total_rows = start_row;
	auto row_groups = GetRowGroups();

	auto l = row_groups->Lock();
	idx_t segment_count = row_groups->GetSegmentCount(l);
	if (segment_count == 0) {
		// we have no segments to revert
		return;
	}
	idx_t segment_index;
	// find the segment index that the start row belongs to
	if (!row_groups->TryGetSegmentIndex(l, start_row, segment_index)) {
		// revert from the last segment
		segment_index = segment_count - 1;
	}
	auto &segment = *row_groups->GetSegmentByIndex(l, UnsafeNumericCast<int64_t>(segment_index));
	if (segment.row_start == start_row) {
		// we are truncating exactly this row group - erase it entirely
		row_groups->EraseSegments(l, segment_index);
	} else {
		// we need to truncate within a row group
		// remove any segments AFTER this segment: they should be deleted entirely
		row_groups->EraseSegments(l, segment_index + 1);

		segment.next = nullptr;
		segment.node->RevertAppend(start_row - segment.row_start);
	}
}

void RowGroupCollection::CleanupAppend(transaction_t lowest_transaction, idx_t start, idx_t count) {
	auto row_groups = GetRowGroups();
	auto row_group = row_groups->GetSegment(start);
	D_ASSERT(row_group);
	idx_t current_row = start;
	idx_t remaining = count;
	while (true) {
		auto &current_row_group = *row_group->node;
		idx_t start_in_row_group = current_row - row_group->row_start;
		idx_t append_count = MinValue<idx_t>(current_row_group.count - start_in_row_group, remaining);

		current_row_group.CleanupAppend(lowest_transaction, start_in_row_group, append_count);

		current_row += append_count;
		remaining -= append_count;
		if (remaining == 0) {
			break;
		}
		row_group = row_groups->GetNextSegment(*row_group);
	}
}

bool RowGroupCollection::IsPersistent() const {
	auto row_groups = GetRowGroups();
	for (auto &row_group : row_groups->Segments()) {
		if (!row_group.IsPersistent()) {
			return false;
		}
	}
	return true;
}

void RowGroupCollection::MergeStorage(RowGroupCollection &data, optional_ptr<DataTable> table,
                                      optional_ptr<StorageCommitState> commit_state) {
	D_ASSERT(data.types == types);
	auto start_index = GetBaseRowId() + total_rows.load();
	auto index = start_index;
	auto segments = data.GetRowGroups()->MoveSegments();
	auto row_groups = GetRowGroups();

	// check if the row groups we are merging are optimistically written
	// if all row groups are optimistically written we keep around the block pointers
	unique_ptr<PersistentCollectionData> row_group_data;
	idx_t optimistically_written_count = 0;
	if (commit_state) {
		for (auto &entry : segments) {
			auto &row_group = *entry->node;
			if (!row_group.IsPersistent()) {
				break;
			}
			optimistically_written_count += row_group.count;
		}
		if (optimistically_written_count > 0) {
			row_group_data = make_uniq<PersistentCollectionData>();
		}
	}
	for (auto &entry : segments) {
		auto &row_group = entry->node;
		row_group->MoveToCollection(*this);

		if (commit_state && (index - start_index) < optimistically_written_count) {
			// serialize the block pointers of this row group
			auto persistent_data = row_group->SerializeRowGroupInfo(index);
			persistent_data.types = types;
			row_group_data->row_group_data.push_back(std::move(persistent_data));
		}
		index += row_group->count;
		row_groups->AppendSegment(std::move(row_group));
	}
	if (commit_state && optimistically_written_count > 0) {
		// if we have serialized the row groups - push the serialized block pointers into the commit state
		commit_state->AddRowGroupData(*table, start_index, optimistically_written_count, std::move(row_group_data));
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
	auto row_groups = GetRowGroups();
	do {
		idx_t start = pos;
		auto row_group = row_groups->GetSegment(UnsafeNumericCast<idx_t>(ids[start]));

		auto &current_row_group = *row_group->node;
		for (pos++; pos < count; pos++) {
			D_ASSERT(ids[pos] >= 0);
			// check if this id still belongs to this row group
			if (idx_t(ids[pos]) < row_group->row_start) {
				// id is before row_group start -> it does not
				break;
			}
			if (idx_t(ids[pos]) >= row_group->row_start + current_row_group.count) {
				// id is after row group end -> it does not
				break;
			}
		}
		delete_count += current_row_group.Delete(transaction, table, ids + start, pos - start, row_group->row_start);
	} while (pos < count);

	return delete_count;
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
optional_ptr<SegmentNode<RowGroup>> RowGroupCollection::NextUpdateRowGroup(RowGroupSegmentTree &row_groups, row_t *ids,
                                                                           idx_t &pos, idx_t count) const {
	auto row_group = row_groups.GetSegment(UnsafeNumericCast<idx_t>(ids[pos]));

	auto &current_row_group = *row_group->node;
	auto row_start = row_group->row_start;
	row_t base_id = UnsafeNumericCast<row_t>(
	    row_start + ((UnsafeNumericCast<idx_t>(ids[pos]) - row_start) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE));
	auto max_id =
	    MinValue<row_t>(base_id + STANDARD_VECTOR_SIZE, UnsafeNumericCast<row_t>(row_start + current_row_group.count));
	for (pos++; pos < count; pos++) {
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
	return row_group;
}

void RowGroupCollection::Update(TransactionData transaction, DataTable &data_table, row_t *ids,
                                const vector<PhysicalIndex> &column_ids, DataChunk &updates) {
	D_ASSERT(updates.size() >= 1);
	idx_t pos = 0;
	auto row_groups = GetRowGroups();
	do {
		idx_t start = pos;
		auto row_group = NextUpdateRowGroup(*row_groups, ids, pos, updates.size());

		auto &current_row_group = *row_group->node;
		current_row_group.Update(transaction, data_table, updates, ids, start, pos - start, column_ids,
		                         row_group->row_start);

		auto l = stats.GetLock();
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto column_id = column_ids[i];
			stats.MergeStats(*l, column_id.index, *current_row_group.GetStatistics(column_id.index));
		}
	} while (pos < updates.size());
}

void RowGroupCollection::RemoveFromIndexes(const QueryContext &context, TableIndexList &indexes,
                                           Vector &row_identifiers, idx_t count) {
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);

	// Collect all Indexed columns on the table.
	unordered_set<column_t> indexed_column_id_set;
	indexes.Scan([&](Index &index) {
		auto &set = index.GetColumnIdSet();
		indexed_column_id_set.insert(set.begin(), set.end());
		return false;
	});

	// If we are in WAL replay, delete data will be buffered, and so we sort the column_ids
	// since the sorted form will be the mapping used to get back physical IDs from the buffered index chunk.
	vector<StorageIndex> column_ids;
	for (auto &col : indexed_column_id_set) {
		column_ids.emplace_back(col);
	}
	sort(column_ids.begin(), column_ids.end());

	vector<LogicalType> column_types;
	for (auto &col : column_ids) {
		column_types.push_back(types[col.GetPrimaryIndex()]);
	}

	// Initialize the fetch state. Only use indexed columns.
	TableScanState state;
	auto column_ids_copy = column_ids;
	state.Initialize(std::move(column_ids_copy));
	state.table_state.max_row = GetBaseRowId() + total_rows;

	DataChunk fetch_chunk;
	fetch_chunk.Initialize(GetAllocator(), column_types);

	// Used for index value removal.
	// Contains all columns but only initializes indexed ones.
	DataChunk result_chunk;
	auto fetched_columns = vector<bool>(types.size(), false);
	result_chunk.Initialize(GetAllocator(), types, fetched_columns);

	// Now set all to-be-fetched columns.
	for (auto &col : indexed_column_id_set) {
		fetched_columns[col] = true;
	}

	// Iterate over the row ids.
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	auto row_groups = GetRowGroups();
	for (idx_t r = 0; r < count;) {
		fetch_chunk.Reset();
		result_chunk.Reset();

		// Figure out which row_group to fetch from.
		auto row_id = row_ids[r];
		auto row_group = row_groups->GetSegment(UnsafeNumericCast<idx_t>(row_id));

		auto &current_row_group = *row_group->node;
		auto row_start = row_group->row_start;
		auto row_group_vector_idx = (UnsafeNumericCast<idx_t>(row_id) - row_start) / STANDARD_VECTOR_SIZE;
		auto base_row_id = row_group_vector_idx * STANDARD_VECTOR_SIZE + row_start;

		// Fetch the current vector into fetch_chunk.
		state.table_state.Initialize(context, GetTypes());
		current_row_group.InitializeScanWithOffset(state.table_state, *row_group, row_group_vector_idx);
		current_row_group.ScanCommitted(state.table_state, fetch_chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		fetch_chunk.Verify();

		// Check for any remaining row ids, if they also fall into this vector.
		// We try to fetch as many rows as possible at the same time.
		idx_t sel_count = 0;
		for (; r < count; r++) {
			idx_t current_row = idx_t(row_ids[r]);
			if (current_row < base_row_id || current_row >= base_row_id + fetch_chunk.size()) {
				// This row id does not fall into the current chunk.
				break;
			}
			auto row_in_vector = current_row - base_row_id;
			D_ASSERT(row_in_vector < fetch_chunk.size());
			sel.set_index(sel_count++, row_in_vector);
		}
		D_ASSERT(sel_count > 0);

		// Reference the necessary columns of the fetch_chunk.
		idx_t fetch_idx = 0;
		for (idx_t j = 0; j < types.size(); j++) {
			if (fetched_columns[j]) {
				result_chunk.data[j].Reference(fetch_chunk.data[fetch_idx++]);
				continue;
			}
			result_chunk.data[j].Reference(Value(types[j]));
		}
		result_chunk.SetCardinality(fetch_chunk);

		// Slice the vector with all rows that are present in this vector.
		// If the index is bound, delete the data. If unbound, buffer into unbound_index.
		result_chunk.Slice(sel, sel_count);
		indexes.Scan([&](Index &index) {
			if (index.IsBound()) {
				index.Cast<BoundIndex>().Delete(result_chunk, row_identifiers);
				return false;
			}
			// Buffering takes only the indexed columns in ordering of the column_ids mapping.
			DataChunk index_column_chunk;
			index_column_chunk.InitializeEmpty(column_types);
			for (idx_t i = 0; i < column_types.size(); i++) {
				auto col_id = column_ids[i].GetPrimaryIndex();
				index_column_chunk.data[i].Reference(result_chunk.data[col_id]);
			}
			index_column_chunk.SetCardinality(result_chunk.size());
			auto &unbound_index = index.Cast<UnboundIndex>();
			unbound_index.BufferChunk(index_column_chunk, row_identifiers, column_ids, BufferedIndexReplay::DEL_ENTRY);
			return false;
		});
	}
}

void RowGroupCollection::UpdateColumn(TransactionData transaction, DataTable &data_table, Vector &row_ids,
                                      const vector<column_t> &column_path, DataChunk &updates) {
	D_ASSERT(updates.size() >= 1);
	auto ids = FlatVector::GetData<row_t>(row_ids);
	idx_t pos = 0;
	auto row_groups = GetRowGroups();
	do {
		idx_t start = pos;
		auto row_group = NextUpdateRowGroup(*row_groups, ids, pos, updates.size());
		auto &current_row_group = *row_group->node;
		current_row_group.UpdateColumn(transaction, data_table, updates, row_ids, start, pos - start, column_path,
		                               row_group->row_start);

		auto lock = stats.GetLock();
		auto primary_column_idx = column_path[0];
		current_row_group.MergeIntoStatistics(primary_column_idx,
		                                      stats.GetStats(*lock, primary_column_idx).Statistics());
	} while (pos < updates.size());
}

//===--------------------------------------------------------------------===//
// Checkpoint State
//===--------------------------------------------------------------------===//
struct CollectionCheckpointState {
	CollectionCheckpointState(RowGroupCollection &collection, TableDataWriter &writer,
	                          const vector<shared_ptr<SegmentNode<RowGroup>>> &segments, TableStatistics &global_stats)
	    : collection(collection), writer(writer), executor(writer.CreateTaskExecutor()), segments(segments),
	      global_stats(global_stats) {
		writers.resize(segments.size());
		write_data.resize(segments.size());
	}

	RowGroupCollection &collection;
	TableDataWriter &writer;
	unique_ptr<TaskExecutor> executor;
	const vector<shared_ptr<SegmentNode<RowGroup>>> &segments;
	vector<unique_ptr<RowGroupWriter>> writers;
	vector<RowGroupWriteData> write_data;
	TableStatistics &global_stats;
};

class BaseCheckpointTask : public BaseExecutorTask {
public:
	explicit BaseCheckpointTask(CollectionCheckpointState &checkpoint_state)
	    : BaseExecutorTask(*checkpoint_state.executor), checkpoint_state(checkpoint_state) {
	}

protected:
	CollectionCheckpointState &checkpoint_state;
};

class CheckpointTask : public BaseCheckpointTask {
public:
	CheckpointTask(CollectionCheckpointState &checkpoint_state, idx_t index)
	    : BaseCheckpointTask(checkpoint_state), index(index) {
	}

	void ExecuteTask() override {
		auto &entry = checkpoint_state.segments[index];
		auto &row_group = *entry->node;
		checkpoint_state.writers[index] = checkpoint_state.writer.GetRowGroupWriter(row_group);
		checkpoint_state.write_data[index] = row_group.WriteToDisk(*checkpoint_state.writers[index]);
	}

	string TaskType() const override {
		return "CheckpointTask";
	}

private:
	idx_t index;
};

//===--------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//
struct VacuumState {
	bool can_vacuum_deletes = false;
	idx_t row_start = 0;
	idx_t next_vacuum_idx = 0;
	vector<idx_t> row_group_counts;
};

class VacuumTask : public BaseCheckpointTask {
public:
	VacuumTask(CollectionCheckpointState &checkpoint_state, VacuumState &vacuum_state, idx_t segment_idx,
	           idx_t merge_count, idx_t target_count, idx_t merge_rows)
	    : BaseCheckpointTask(checkpoint_state), vacuum_state(vacuum_state), segment_idx(segment_idx),
	      merge_count(merge_count), target_count(target_count), merge_rows(merge_rows) {
	}

	void ExecuteTask() override {
		auto &collection = checkpoint_state.collection;
		const idx_t row_group_size = collection.GetRowGroupSize();
		auto &types = collection.GetTypes();
		// create the new set of target row groups (initially empty)
		vector<unique_ptr<RowGroup>> new_row_groups;
		vector<idx_t> append_counts;
		idx_t row_group_rows = merge_rows;
		for (idx_t target_idx = 0; target_idx < target_count; target_idx++) {
			idx_t current_row_group_rows = MinValue<idx_t>(row_group_rows, row_group_size);
			auto new_row_group = make_uniq<RowGroup>(collection, current_row_group_rows);
			new_row_group->InitializeEmpty(types, ColumnDataType::MAIN_TABLE);
			new_row_groups.push_back(std::move(new_row_group));
			append_counts.push_back(0);

			row_group_rows -= current_row_group_rows;
		}

		DataChunk scan_chunk;
		scan_chunk.Initialize(Allocator::DefaultAllocator(), types);

		vector<StorageIndex> column_ids;
		for (idx_t c = 0; c < types.size(); c++) {
			column_ids.emplace_back(c);
		}

		idx_t current_append_idx = 0;

		// fill the new row group with the merged rows
		TableAppendState append_state;
		new_row_groups[current_append_idx]->InitializeAppend(append_state.row_group_append_state);

		TableScanState scan_state;
		scan_state.Initialize(column_ids);
		scan_state.table_state.Initialize(QueryContext(), types);
		scan_state.table_state.max_row = idx_t(-1);
		idx_t merged_groups = 0;
		idx_t total_row_groups = vacuum_state.row_group_counts.size();
		for (idx_t c_idx = segment_idx; merged_groups < merge_count && c_idx < total_row_groups; c_idx++) {
			if (vacuum_state.row_group_counts[c_idx] == 0) {
				continue;
			}
			merged_groups++;

			auto &current_row_group = *checkpoint_state.segments[c_idx]->node;

			current_row_group.InitializeScan(scan_state.table_state, *checkpoint_state.segments[c_idx]);
			while (true) {
				scan_chunk.Reset();

				current_row_group.ScanCommitted(scan_state.table_state, scan_chunk,
				                                TableScanType::TABLE_SCAN_LATEST_COMMITTED_ROWS);
				if (scan_chunk.size() == 0) {
					break;
				}
				scan_chunk.Flatten();
				idx_t remaining = scan_chunk.size();
				while (remaining > 0) {
					idx_t append_count = MinValue<idx_t>(remaining, row_group_size - append_counts[current_append_idx]);
					new_row_groups[current_append_idx]->Append(append_state.row_group_append_state, scan_chunk,
					                                           append_count);
					append_counts[current_append_idx] += append_count;
					remaining -= append_count;
					const bool row_group_full = append_counts[current_append_idx] == row_group_size;
					const bool last_row_group = current_append_idx + 1 >= new_row_groups.size();
					if (remaining > 0 || (row_group_full && !last_row_group)) {
						// move to the next row group
						current_append_idx++;
						new_row_groups[current_append_idx]->InitializeAppend(append_state.row_group_append_state);
						// slice chunk for the next append
						scan_chunk.Slice(append_count, remaining);
					}
				}
			}
			// drop the row group after merging
			current_row_group.CommitDrop();
			checkpoint_state.segments[c_idx]->node.reset();
		}
		idx_t total_append_count = 0;
		for (idx_t target_idx = 0; target_idx < target_count; target_idx++) {
			auto &row_group = new_row_groups[target_idx];
			row_group->Verify();

			// assign the new row group to the current segment
			checkpoint_state.segments[segment_idx + target_idx]->node = std::move(row_group);
			total_append_count += append_counts[target_idx];
		}
		if (total_append_count != merge_rows) {
			throw InternalException("Mismatch in row group count vs verify count in RowGroupCollection::Checkpoint");
		}
		// merging is complete - execute checkpoint tasks of the target row groups
		for (idx_t i = 0; i < target_count; i++) {
			auto checkpoint_task = collection.GetCheckpointTask(checkpoint_state, segment_idx + i);
			checkpoint_task->ExecuteTask();
		}
	}

	string TaskType() const override {
		return "VacuumTask";
	}

private:
	VacuumState &vacuum_state;
	idx_t segment_idx;
	idx_t merge_count;
	idx_t target_count;
	idx_t merge_rows;
};

void RowGroupCollection::InitializeVacuumState(CollectionCheckpointState &checkpoint_state, VacuumState &state,
                                               const vector<shared_ptr<SegmentNode<RowGroup>>> &segments) {
	auto checkpoint_type = checkpoint_state.writer.GetCheckpointType();
	bool vacuum_is_allowed = checkpoint_type != CheckpointType::CONCURRENT_CHECKPOINT;
	// currently we can only vacuum deletes if we are doing a full checkpoint and there are no indexes
	state.can_vacuum_deletes = info->GetIndexes().Empty() && vacuum_is_allowed;
	if (!state.can_vacuum_deletes) {
		return;
	}
	// obtain the set of committed row counts for each row group
	state.row_group_counts.reserve(segments.size());
	for (auto &entry : segments) {
		auto &row_group = *entry->node;
		auto row_group_count = row_group.GetCommittedRowCount();
		if (row_group_count == 0) {
			// empty row group - we can drop it entirely
			row_group.CommitDrop();
			entry->node.reset();
		}
		state.row_group_counts.push_back(row_group_count);
	}
}

bool RowGroupCollection::ScheduleVacuumTasks(CollectionCheckpointState &checkpoint_state, VacuumState &state,
                                             idx_t segment_idx, bool schedule_vacuum) {
	static constexpr const idx_t MAX_MERGE_COUNT = 3;

	if (!state.can_vacuum_deletes) {
		// we cannot vacuum deletes - cannot vacuum
		return false;
	}
	if (segment_idx < state.next_vacuum_idx) {
		// this segment is being vacuumed by a previously scheduled task
		return true;
	}
	if (state.row_group_counts[segment_idx] == 0) {
		// segment was already dropped - skip
		D_ASSERT(!checkpoint_state.segments[segment_idx]->node);
		return false;
	}
	if (!schedule_vacuum) {
		return false;
	}
	idx_t merge_rows;
	idx_t next_idx = 0;
	idx_t merge_count = 0;
	idx_t target_count = 0;
	bool perform_merge = false;
	// check if we can merge row groups adjacent to the current segment_idx
	// we try merging row groups into batches of 1-3 row groups
	// our goal is to reduce the amount of row groups
	// hence we target_count should be less than merge_count for a merge to be worth it
	// we greedily prefer to merge to the lowest target_count
	// i.e. we prefer to merge 2 row groups into 1, than 3 row groups into 2
	const idx_t row_group_size = GetRowGroupSize();
	for (target_count = 1; target_count <= MAX_MERGE_COUNT; target_count++) {
		auto total_target_size = target_count * row_group_size;
		merge_count = 0;
		merge_rows = 0;
		for (next_idx = segment_idx; next_idx < checkpoint_state.segments.size(); next_idx++) {
			if (state.row_group_counts[next_idx] == 0) {
				continue;
			}
			if (merge_rows + state.row_group_counts[next_idx] > total_target_size) {
				// does not fit
				break;
			}
			// we can merge this row group together with the other row group
			merge_rows += state.row_group_counts[next_idx];
			merge_count++;
		}
		if (next_idx == checkpoint_state.segments.size()) {
			// in order to prevent poor performance when performing small appends, we only merge row groups at the end
			// if we can reach a "target" size of twice the current size, or the max row group size
			// this is to prevent repeated expensive checkpoints where:
			// we have a row group with 100K rows
			// merge it with a row group with 1 row, creating a row group with 100K+1 rows
			// merge it with a row group with 1 row, creating a row group with 100K+2 rows
			// etc. This leads to constant rewriting of the original 100K rows.
			idx_t minimum_target =
			    MinValue<idx_t>(state.row_group_counts[segment_idx] * 2, row_group_size) * target_count;
			if (merge_rows >= STANDARD_VECTOR_SIZE && merge_rows < minimum_target) {
				// we haven't reached the minimum target - don't do this vacuum
				next_idx = segment_idx + 1;
				continue;
			}
		}
		if (target_count < merge_count) {
			// we can reduce "merge_count" row groups to "target_count"
			// perform the merge at this level
			perform_merge = true;
			break;
		}
	}
	if (!perform_merge) {
		return false;
	}
	// schedule the vacuum task
	DUCKDB_LOG(checkpoint_state.writer.GetDatabase(), CheckpointLogType, GetAttached(), *info, segment_idx, merge_count,
	           target_count, merge_rows, state.row_start);
	auto vacuum_task =
	    make_uniq<VacuumTask>(checkpoint_state, state, segment_idx, merge_count, target_count, merge_rows);
	checkpoint_state.executor->ScheduleTask(std::move(vacuum_task));
	// skip vacuuming by the row groups we have merged
	state.next_vacuum_idx = next_idx;
	state.row_start += merge_rows;
	return true;
}

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
unique_ptr<CheckpointTask> RowGroupCollection::GetCheckpointTask(CollectionCheckpointState &checkpoint_state,
                                                                 idx_t segment_idx) {
	return make_uniq<CheckpointTask>(checkpoint_state, segment_idx);
}

void RowGroupCollection::Checkpoint(TableDataWriter &writer, TableStatistics &global_stats) {
	auto row_groups = GetRowGroups();
	auto l = row_groups->Lock();
	auto &segments = row_groups->ReferenceSegments(l);

	CollectionCheckpointState checkpoint_state(*this, writer, segments, global_stats);

	VacuumState vacuum_state;
	InitializeVacuumState(checkpoint_state, vacuum_state, segments);

	try {
		// schedule tasks
		idx_t total_vacuum_tasks = 0;
		auto max_vacuum_tasks = DBConfig::GetSetting<MaxVacuumTasksSetting>(writer.GetDatabase());
		for (idx_t segment_idx = 0; segment_idx < segments.size(); segment_idx++) {
			auto &entry = segments[segment_idx];
			auto vacuum_tasks =
			    ScheduleVacuumTasks(checkpoint_state, vacuum_state, segment_idx, total_vacuum_tasks < max_vacuum_tasks);
			if (vacuum_tasks) {
				// vacuum tasks were scheduled - don't schedule a checkpoint task yet
				total_vacuum_tasks++;
				continue;
			}
			if (!entry->node) {
				// row group was vacuumed/dropped - skip
				continue;
			}
			// schedule a checkpoint task for this row group
			auto &row_group = *entry->node;
			if (vacuum_state.row_start != entry->row_start) {
				row_group.MoveToCollection(*this);
			} else if (!RefersToSameObject(row_group.GetCollection(), *this)) {
				throw InternalException("RowGroup Vacuum - row group collection of row group changed");
			}
			if (writer.GetCheckpointType() != CheckpointType::VACUUM_ONLY) {
				DUCKDB_LOG(checkpoint_state.writer.GetDatabase(), CheckpointLogType, GetAttached(), *info, segment_idx,
				           row_group, vacuum_state.row_start);
				auto checkpoint_task = GetCheckpointTask(checkpoint_state, segment_idx);
				checkpoint_state.executor->ScheduleTask(std::move(checkpoint_task));
			}
			vacuum_state.row_start += row_group.count;
		}
	} catch (const std::exception &e) {
		ErrorData error(e);
		checkpoint_state.executor->PushError(std::move(error));
		checkpoint_state.executor->WorkOnTasks(); // ensure all tasks have completed first before rethrowing
		throw;
	}
	// all tasks have been successfully scheduled - execute tasks until we are done
	checkpoint_state.executor->WorkOnTasks();

	// no errors - finalize the row groups
	// if the table already exists on disk - check if all row groups have stayed the same
	if (DBConfig::GetSetting<ExperimentalMetadataReuseSetting>(writer.GetDatabase()) && metadata_pointer.IsValid()) {
		bool table_has_changes = false;
		for (idx_t segment_idx = 0; segment_idx < segments.size(); segment_idx++) {
			auto &entry = segments[segment_idx];
			if (!entry->node) {
				table_has_changes = true;
				break;
			}
			auto &write_state = checkpoint_state.write_data[segment_idx];
			if (!write_state.reuse_existing_metadata_blocks) {
				table_has_changes = true;
				break;
			}
		}
		if (!table_has_changes) {
			// table is unmodified and already exists on disk
			// we can directly re-use the metadata pointer
			// mark all blocks associated with row groups as still being in-use
			auto &metadata_manager = writer.GetMetadataManager();
			for (idx_t segment_idx = 0; segment_idx < segments.size(); segment_idx++) {
				auto &entry = segments[segment_idx];
				auto &row_group = *entry->node;
				auto &write_state = checkpoint_state.write_data[segment_idx];
				metadata_manager.ClearModifiedBlocks(row_group.GetColumnStartPointers());
				D_ASSERT(write_state.reuse_existing_metadata_blocks);
				vector<MetaBlockPointer> extra_metadata_block_pointers;
				extra_metadata_block_pointers.reserve(write_state.existing_extra_metadata_blocks.size());
				for (auto &block_pointer : write_state.existing_extra_metadata_blocks) {
					extra_metadata_block_pointers.emplace_back(block_pointer, 0);
				}
				metadata_manager.ClearModifiedBlocks(extra_metadata_block_pointers);
				metadata_manager.ClearModifiedBlocks(row_group.GetDeletesPointers());
			}
			writer.WriteUnchangedTable(metadata_pointer, total_rows.load());
			return;
		}
	}

	// not all segments have stayed the same - we need to make a new segment tree with the new set of segments
	auto new_row_groups = make_shared_ptr<RowGroupSegmentTree>(*this, GetBaseRowId());

	idx_t new_total_rows = 0;
	for (idx_t segment_idx = 0; segment_idx < segments.size(); segment_idx++) {
		auto &entry = segments[segment_idx];
		if (!entry->node) {
			// row group was vacuumed/dropped - skip
			continue;
		}
		auto &row_group = *entry->node;
		auto &row_group_writer = checkpoint_state.writers[segment_idx];
		if (!row_group_writer) {
			// row group was not checkpointed - this can happen if compressing is disabled for in-memory tables
			D_ASSERT(writer.GetCheckpointType() == CheckpointType::VACUUM_ONLY);
			new_row_groups->AppendSegment(l, entry->node);
			new_total_rows += row_group.count;
			continue;
		}
		auto &row_group_write_data = checkpoint_state.write_data[segment_idx];
		idx_t row_start = new_total_rows;
		bool metadata_reuse = row_group_write_data.reuse_existing_metadata_blocks;
		auto new_row_group = std::move(row_group_write_data.result_row_group);
		if (!new_row_group) {
			// row group was unchanged - emit previous row group
			new_row_group = entry->node;
		}
		auto pointer =
		    row_group.Checkpoint(std::move(row_group_write_data), *row_group_writer, global_stats, row_start);

		auto debug_verify_blocks = DBConfig::GetSetting<DebugVerifyBlocksSetting>(GetAttached().GetDatabase()) &&
		                           dynamic_cast<SingleFileTableDataWriter *>(&checkpoint_state.writer) != nullptr;
		RowGroupPointer pointer_copy;
		if (debug_verify_blocks) {
			pointer_copy = pointer;
		}
		writer.AddRowGroup(std::move(pointer), std::move(row_group_writer));
		new_row_groups->AppendSegment(l, std::move(new_row_group));
		new_total_rows += row_group.count;

		if (debug_verify_blocks) {
			if (!pointer_copy.has_metadata_blocks) {
				throw InternalException("Checkpointing should always remember metadata blocks");
			}
			if (metadata_reuse && pointer_copy.data_pointers != row_group.GetColumnStartPointers()) {
				throw InternalException("Colum start pointers changed during metadata reuse");
			}

			// Capture blocks that have been written
			vector<MetaBlockPointer> all_written_blocks = pointer_copy.data_pointers;
			vector<MetaBlockPointer> all_metadata_blocks;
			for (auto &block : pointer_copy.extra_metadata_blocks) {
				all_written_blocks.emplace_back(block, 0);
				all_metadata_blocks.emplace_back(block, 0);
			}

			// Verify that we can load the metadata correctly again
			vector<MetaBlockPointer> all_quick_read_blocks;
			for (auto &ptr : row_group.GetColumnStartPointers()) {
				all_quick_read_blocks.emplace_back(ptr);
				if (metadata_reuse && !block_manager.GetMetadataManager().BlockHasBeenCleared(ptr)) {
					throw InternalException("Found column start block that was not cleared");
				}
			}
			auto extra_metadata_blocks = row_group.GetOrComputeExtraMetadataBlocks(/* force_compute: */ true);
			for (auto &ptr : extra_metadata_blocks) {
				auto block_pointer = MetaBlockPointer(ptr, 0);
				all_quick_read_blocks.emplace_back(block_pointer);
				if (metadata_reuse && !block_manager.GetMetadataManager().BlockHasBeenCleared(block_pointer)) {
					throw InternalException("Found extra metadata block that was not cleared");
				}
			}

			// Deserialize all columns to check if the quick read via GetOrComputeExtraMetadataBlocks was correct
			vector<MetaBlockPointer> all_full_read_blocks;
			auto column_start_pointers = row_group.GetColumnStartPointers();
			auto &types = row_group.GetCollection().GetTypes();
			auto &metadata_manager = row_group.GetCollection().GetMetadataManager();
			for (idx_t i = 0; i < column_start_pointers.size(); i++) {
				MetadataReader reader(metadata_manager, column_start_pointers[i], &all_full_read_blocks);
				ColumnData::Deserialize(GetBlockManager(), GetTableInfo(), i, reader, types[i]);
			}

			// Derive sets of blocks to compare
			set<idx_t> all_written_block_ids;
			for (auto &ptr : all_written_blocks) {
				all_written_block_ids.insert(ptr.block_pointer);
			}
			set<idx_t> all_quick_read_block_ids;
			for (auto &ptr : all_quick_read_blocks) {
				all_quick_read_block_ids.insert(ptr.block_pointer);
			}
			set<idx_t> all_full_read_block_ids;
			for (auto &ptr : all_full_read_blocks) {
				all_full_read_block_ids.insert(ptr.block_pointer);
			}
			if (all_written_block_ids != all_quick_read_block_ids ||
			    all_quick_read_block_ids != all_full_read_block_ids) {
				std::stringstream oss;
				oss << "Written: ";
				for (auto &block : all_written_blocks) {
					oss << block << ", ";
				}
				oss << "\n";
				oss << "Quick read: ";
				for (auto &block : all_quick_read_blocks) {
					oss << block << ", ";
				}
				oss << "\n";
				oss << "Full read: ";
				for (auto &block : all_full_read_blocks) {
					oss << block << ", ";
				}
				oss << "\n";

				throw InternalException("Reloading blocks just written does not yield same blocks: " + oss.str());
			}
		}
	}
	l.Release();
	// override the row group segment tree
	SetRowGroups(std::move(new_row_groups));
	total_rows = new_total_rows;
	Verify();
}

//===--------------------------------------------------------------------===//
// Destroy
//===--------------------------------------------------------------------===//

class DestroyTask : public BaseExecutorTask {
public:
	DestroyTask(TaskExecutor &executor, shared_ptr<RowGroup> row_group_p)
	    : BaseExecutorTask(executor), row_group(std::move(row_group_p)) {
	}

	void ExecuteTask() override {
		row_group.reset();
	}

private:
	shared_ptr<RowGroup> row_group;
};

void RowGroupCollection::Destroy() {
	auto l = owned_row_groups->Lock();
	auto &segments = owned_row_groups->ReferenceLoadedSegmentsMutable(l);

	TaskExecutor executor(TaskScheduler::GetScheduler(GetAttached().GetDatabase()));
	for (auto &segment : segments) {
		auto destroy_task = make_uniq<DestroyTask>(executor, std::move(segment->node));
		executor.ScheduleTask(std::move(destroy_task));
	}
	executor.WorkOnTasks();
}

//===--------------------------------------------------------------------===//
// CommitDrop
//===--------------------------------------------------------------------===//
void RowGroupCollection::CommitDropColumn(const idx_t column_index) {
	auto row_groups = GetRowGroups();
	for (auto &row_group : row_groups->Segments()) {
		row_group.CommitDropColumn(column_index);
	}
}

void RowGroupCollection::CommitDropTable() {
	auto row_groups = GetRowGroups();
	for (auto &row_group : row_groups->Segments()) {
		row_group.CommitDrop();
	}
}

//===--------------------------------------------------------------------===//
// GetPartitionStats
//===--------------------------------------------------------------------===//
vector<PartitionStatistics> RowGroupCollection::GetPartitionStats() const {
	vector<PartitionStatistics> result;
	auto row_groups = GetRowGroups();
	for (auto &entry : row_groups->SegmentNodes()) {
		auto &row_group = *entry.node;
		result.push_back(row_group.GetPartitionStats(entry.row_start));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// GetColumnSegmentInfo
//===--------------------------------------------------------------------===//
vector<ColumnSegmentInfo> RowGroupCollection::GetColumnSegmentInfo(const QueryContext &context) {
	vector<ColumnSegmentInfo> result;
	auto row_groups = GetRowGroups();
	auto lock = row_groups->Lock();
	for (auto &node : row_groups->SegmentNodes(lock)) {
		auto &row_group = *node.node;
		row_group.GetColumnSegmentInfo(context, node.index, result);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Alter
//===--------------------------------------------------------------------===//
shared_ptr<RowGroupCollection> RowGroupCollection::AddColumn(ClientContext &context, ColumnDefinition &new_column,
                                                             ExpressionExecutor &default_executor) {
	idx_t new_column_idx = types.size();
	auto new_types = types;
	new_types.push_back(new_column.GetType());
	auto result = make_shared_ptr<RowGroupCollection>(info, block_manager, std::move(new_types), GetBaseRowId(),
	                                                  total_rows.load(), row_group_size);

	DataChunk dummy_chunk;
	Vector default_vector(new_column.GetType());

	result->stats.InitializeAddColumn(stats, new_column.GetType());
	auto lock = result->stats.GetLock();
	auto &new_column_stats = result->stats.GetStats(*lock, new_column_idx);

	// fill the column with its DEFAULT value, or NULL if none is specified
	auto new_stats = make_uniq<SegmentStatistics>(new_column.GetType());
	auto row_groups = GetRowGroups();
	auto result_row_groups = result->GetRowGroups();
	for (auto &current_row_group : row_groups->Segments()) {
		auto new_row_group = current_row_group.AddColumn(*result, new_column, default_executor, default_vector);
		// merge in the statistics
		new_row_group->MergeIntoStatistics(new_column_idx, new_column_stats.Statistics());

		result_row_groups->AppendSegment(std::move(new_row_group));
	}

	return result;
}

shared_ptr<RowGroupCollection> RowGroupCollection::RemoveColumn(idx_t col_idx) {
	D_ASSERT(col_idx < types.size());
	auto new_types = types;
	new_types.erase_at(col_idx);

	auto result = make_shared_ptr<RowGroupCollection>(info, block_manager, std::move(new_types), GetBaseRowId(),
	                                                  total_rows.load(), row_group_size);
	result->stats.InitializeRemoveColumn(stats, col_idx);

	auto result_lock = result->stats.GetLock();
	result->stats.DestroyTableSample(*result_lock);

	auto row_groups = GetRowGroups();
	auto result_row_groups = result->GetRowGroups();
	for (auto &current_row_group : row_groups->Segments()) {
		auto new_row_group = current_row_group.RemoveColumn(*result, col_idx);
		result_row_groups->AppendSegment(std::move(new_row_group));
	}
	return result;
}

shared_ptr<RowGroupCollection> RowGroupCollection::AlterType(ClientContext &context, idx_t changed_idx,
                                                             const LogicalType &target_type,
                                                             vector<StorageIndex> bound_columns,
                                                             Expression &cast_expr) {
	D_ASSERT(changed_idx < types.size());
	auto new_types = types;
	new_types[changed_idx] = target_type;

	auto result = make_shared_ptr<RowGroupCollection>(info, block_manager, std::move(new_types), GetBaseRowId(),
	                                                  total_rows.load(), row_group_size);
	result->stats.InitializeAlterType(stats, changed_idx, target_type);

	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < bound_columns.size(); i++) {
		if (bound_columns[i].IsRowIdColumn()) {
			scan_types.emplace_back(LogicalType::ROW_TYPE);
		} else {
			scan_types.push_back(types[bound_columns[i].GetPrimaryIndex()]);
		}
	}
	DataChunk scan_chunk;
	scan_chunk.Initialize(GetAllocator(), scan_types);

	ExpressionExecutor executor(context);
	executor.AddExpression(cast_expr);

	TableScanState scan_state;
	scan_state.Initialize(bound_columns);
	scan_state.table_state.max_row = GetBaseRowId() + total_rows;

	// now alter the type of the column within all of the row_groups individually
	auto lock = result->stats.GetLock();
	auto &changed_stats = result->stats.GetStats(*lock, changed_idx);
	auto row_groups = GetRowGroups();
	auto result_row_groups = result->GetRowGroups();
	for (auto &node : row_groups->SegmentNodes()) {
		auto &current_row_group = *node.node;
		auto new_row_group = current_row_group.AlterType(*result, target_type, changed_idx, executor,
		                                                 scan_state.table_state, node, scan_chunk);
		new_row_group->MergeIntoStatistics(changed_idx, changed_stats.Statistics());
		result_row_groups->AppendSegment(std::move(new_row_group));
	}
	return result;
}

void RowGroupCollection::VerifyNewConstraint(const QueryContext &context, DataTable &parent,
                                             const BoundConstraint &constraint) {
	if (total_rows == 0) {
		return;
	}

	// Scan the original table for NULL values.
	auto &not_null_constraint = constraint.Cast<BoundNotNullConstraint>();
	vector<LogicalType> scan_types;
	auto physical_index = not_null_constraint.index.index;
	D_ASSERT(physical_index < types.size());

	scan_types.push_back(types[physical_index]);
	DataChunk scan_chunk;
	scan_chunk.Initialize(GetAllocator(), scan_types);

	vector<StorageIndex> column_ids;
	column_ids.emplace_back(physical_index);

	// Use SCAN_COMMITTED to scan the latest data.
	CreateIndexScanState state;
	auto scan_type = TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED;
	state.Initialize(column_ids, nullptr);
	InitializeScan(context, state.table_state, column_ids, nullptr);

	InitializeCreateIndexScan(state);

	while (true) {
		scan_chunk.Reset();
		state.table_state.ScanCommitted(scan_chunk, state.segment_lock, scan_type);
		if (scan_chunk.size() == 0) {
			break;
		}

		// Verify the NOT NULL constraint.
		if (VectorOperations::HasNull(scan_chunk.data[0], scan_chunk.size())) {
			auto name = parent.Columns()[physical_index].GetName();
			throw ConstraintException("NOT NULL constraint failed: %s.%s", info->GetTableName(), name);
		}
	}
}

//===--------------------------------------------------------------------===//
// Statistics
//===---------------------------------------------------------------r-----===//
void RowGroupCollection::CopyStats(TableStatistics &other_stats) {
	stats.CopyStats(other_stats);
}

unique_ptr<BaseStatistics> RowGroupCollection::CopyStats(column_t column_id) {
	return stats.CopyStats(column_id);
}

unique_ptr<BlockingSample> RowGroupCollection::GetSample() {
	return nullptr;
}

void RowGroupCollection::SetDistinct(column_t column_id, unique_ptr<DistinctStatistics> distinct_stats) {
	D_ASSERT(column_id != COLUMN_IDENTIFIER_ROW_ID);
	auto stats_lock = stats.GetLock();
	stats.GetStats(*stats_lock, column_id).SetDistinct(std::move(distinct_stats));
}

} // namespace duckdb
