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
		reader.reset();
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
RowGroupCollection::RowGroupCollection(shared_ptr<DataTableInfo> info_p, TableIOManager &io_manager,
                                       vector<LogicalType> types_p, idx_t row_start, idx_t total_rows)
    : RowGroupCollection(std::move(info_p), io_manager.GetBlockManagerForRowData(), std::move(types_p), row_start,
                         total_rows, io_manager.GetRowGroupSize()) {
}

RowGroupCollection::RowGroupCollection(shared_ptr<DataTableInfo> info_p, BlockManager &block_manager,
                                       vector<LogicalType> types_p, idx_t row_start_p, idx_t total_rows_p,
                                       idx_t row_group_size_p)
    : block_manager(block_manager), row_group_size(row_group_size_p), total_rows(total_rows_p), info(std::move(info_p)),
      types(std::move(types_p)), row_start(row_start_p), allocation_size(0) {
	row_groups = make_shared_ptr<RowGroupSegmentTree>(*this);
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

void RowGroupCollection::Initialize(PersistentCollectionData &data) {
	stats.InitializeEmpty(types);
	auto l = row_groups->Lock();
	for (auto &row_group_data : data.row_group_data) {
		auto row_group = make_uniq<RowGroup>(*this, row_group_data);
		row_group->MergeIntoStatistics(stats);
		total_rows += row_group->count;
		row_groups->AppendSegment(l, std::move(row_group));
	}
}

void RowGroupCollection::InitializeEmpty() {
	stats.InitializeEmpty(types);
}

void RowGroupCollection::AppendRowGroup(SegmentLock &l, idx_t start_row) {
	D_ASSERT(start_row >= row_start);
	auto new_row_group = make_uniq<RowGroup>(*this, start_row, 0U);
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
void RowGroupCollection::InitializeScan(CollectionScanState &state, const vector<StorageIndex> &column_ids,
                                        optional_ptr<TableFilterSet> table_filters) {
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

void RowGroupCollection::InitializeScanWithOffset(CollectionScanState &state, const vector<StorageIndex> &column_ids,
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
	for (idx_t i = 0; i < fetch_count; i++) {
		auto row_id = row_ids[i];
		RowGroup *row_group;
		{
			idx_t segment_index;
			auto l = row_groups->Lock();
			if (!row_groups->TryGetSegmentIndex(l, UnsafeNumericCast<idx_t>(row_id), segment_index)) {
				// in parallel append scenarios it is possible for the row_id
				continue;
			}
			row_group = row_groups->GetSegmentByIndex(l, UnsafeNumericCast<int64_t>(segment_index));
		}
		if (!row_group->Fetch(transaction, UnsafeNumericCast<idx_t>(row_id) - row_group->start)) {
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
    : row_group_append_state(*this), total_append_count(0), start_row_group(nullptr), transaction(0, 0),
      hashes(LogicalType::HASH) {
}

TableAppendState::~TableAppendState() {
}

bool RowGroupCollection::IsEmpty() const {
	auto l = row_groups->Lock();
	return IsEmpty(l);
}

bool RowGroupCollection::IsEmpty(SegmentLock &l) const {
	return row_groups->IsEmpty(l);
}

void RowGroupCollection::InitializeAppend(TransactionData transaction, TableAppendState &state) {
	state.row_start = UnsafeNumericCast<row_t>(total_rows.load());
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
	state.transaction = transaction;

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
		if (remaining > 0) {
			// we expect max 1 iteration of this loop (i.e. a single chunk should never overflow more than one
			// row_group)
			D_ASSERT(chunk.size() == remaining + append_count);
			// slice the input chunk
			if (remaining < chunk.size()) {
				chunk.Slice(append_count, remaining);
			}
			// append a new row_group
			new_row_group = true;
			auto next_start = current_row_group->start + state.row_group_append_state.offset_in_row_group;

			auto l = row_groups->Lock();
			AppendRowGroup(l, next_start);
			// set up the append state for this row_group
			auto last_row_group = row_groups->GetLastSegment(l);
			last_row_group->InitializeAppend(state.row_group_append_state);
			continue;
		} else {
			break;
		}
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
		auto append_count = MinValue<idx_t>(remaining, row_group_size - row_group->count);
		row_group->AppendVersionInfo(transaction, append_count);
		remaining -= append_count;
		row_group = row_groups->GetNextSegment(row_group);
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

void RowGroupCollection::RevertAppendInternal(idx_t start_row) {
	total_rows = start_row;

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

	// remove any segments AFTER this segment: they should be deleted entirely
	row_groups->EraseSegments(l, segment_index);

	segment.next = nullptr;
	segment.RevertAppend(start_row);
}

void RowGroupCollection::CleanupAppend(transaction_t lowest_transaction, idx_t start, idx_t count) {
	auto row_group = row_groups->GetSegment(start);
	D_ASSERT(row_group);
	idx_t current_row = start;
	idx_t remaining = count;
	while (true) {
		idx_t start_in_row_group = current_row - row_group->start;
		idx_t append_count = MinValue<idx_t>(row_group->count - start_in_row_group, remaining);

		row_group->CleanupAppend(lowest_transaction, start_in_row_group, append_count);

		current_row += append_count;
		remaining -= append_count;
		if (remaining == 0) {
			break;
		}
		row_group = row_groups->GetNextSegment(row_group);
	}
}

bool RowGroupCollection::IsPersistent() const {
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
	auto start_index = row_start + total_rows.load();
	auto index = start_index;
	auto segments = data.row_groups->MoveSegments();

	// check if the row groups we are merging are optimistically written
	// if all row groups are optimistically written we keep around the block pointers
	unique_ptr<PersistentCollectionData> row_group_data;
	idx_t optimistically_written_count = 0;
	if (commit_state) {
		for (auto &entry : segments) {
			auto &row_group = *entry.node;
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
		auto &row_group = entry.node;
		row_group->MoveToCollection(*this, index);

		if (commit_state && (index - start_index) < optimistically_written_count) {
			// serialize the block pointers of this row group
			auto persistent_data = row_group->SerializeRowGroupInfo();
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
	do {
		idx_t start = pos;
		auto row_group = row_groups->GetSegment(UnsafeNumericCast<idx_t>(ids[start]));
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
	D_ASSERT(updates.size() >= 1);
	idx_t pos = 0;
	do {
		idx_t start = pos;
		auto row_group = row_groups->GetSegment(UnsafeNumericCast<idx_t>(ids[pos]));
		row_t base_id =
		    UnsafeNumericCast<row_t>(row_group->start + ((UnsafeNumericCast<idx_t>(ids[pos]) - row_group->start) /
		                                                 STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE));
		auto max_id = MinValue<row_t>(base_id + STANDARD_VECTOR_SIZE,
		                              UnsafeNumericCast<row_t>(row_group->start + row_group->count));
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

	// Collect all indexed columns.
	unordered_set<column_t> indexed_column_id_set;
	indexes.Scan([&](Index &index) {
		D_ASSERT(index.IsBound());
		auto &set = index.GetColumnIdSet();
		indexed_column_id_set.insert(set.begin(), set.end());
		return false;
	});
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
	state.Initialize(std::move(column_ids));
	state.table_state.max_row = row_start + total_rows;

	// Used for scanning data. Only contains the indexed columns.
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
	for (idx_t r = 0; r < count;) {
		fetch_chunk.Reset();
		result_chunk.Reset();

		// Figure out which row_group to fetch from.
		auto row_id = row_ids[r];
		auto row_group = row_groups->GetSegment(UnsafeNumericCast<idx_t>(row_id));
		auto row_group_vector_idx = (UnsafeNumericCast<idx_t>(row_id) - row_group->start) / STANDARD_VECTOR_SIZE;
		auto base_row_id = row_group_vector_idx * STANDARD_VECTOR_SIZE + row_group->start;

		// Fetch the current vector into fetch_chunk.
		state.table_state.Initialize(GetTypes());
		row_group->InitializeScanWithOffset(state.table_state, row_group_vector_idx);
		row_group->ScanCommitted(state.table_state, fetch_chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
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
		// Then, erase all values from the indexes.
		result_chunk.Slice(sel, sel_count);
		indexes.Scan([&](Index &index) {
			if (index.IsBound()) {
				index.Cast<BoundIndex>().Delete(result_chunk, row_identifiers);
				return false;
			}
			throw MissingExtensionException(
			    "Cannot delete from index '%s', unknown index type '%s'. You need to load the "
			    "extension that provides this index type before table '%s' can be modified.",
			    index.GetIndexName(), index.GetIndexType(), info->GetTableName());
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
	auto row_group = row_groups->GetSegment(UnsafeNumericCast<idx_t>(first_id));
	row_group->UpdateColumn(transaction, updates, row_ids, column_path);

	auto lock = stats.GetLock();
	row_group->MergeIntoStatistics(primary_column_idx, stats.GetStats(*lock, primary_column_idx).Statistics());
}

//===--------------------------------------------------------------------===//
// Checkpoint State
//===--------------------------------------------------------------------===//
struct CollectionCheckpointState {
	CollectionCheckpointState(RowGroupCollection &collection, TableDataWriter &writer,
	                          vector<SegmentNode<RowGroup>> &segments, TableStatistics &global_stats)
	    : collection(collection), writer(writer), executor(writer.GetScheduler()), segments(segments),
	      global_stats(global_stats) {
		writers.resize(segments.size());
		write_data.resize(segments.size());
	}

	RowGroupCollection &collection;
	TableDataWriter &writer;
	TaskExecutor executor;
	vector<SegmentNode<RowGroup>> &segments;
	vector<unique_ptr<RowGroupWriter>> writers;
	vector<RowGroupWriteData> write_data;
	TableStatistics &global_stats;
	mutex write_lock;
};

class BaseCheckpointTask : public BaseExecutorTask {
public:
	explicit BaseCheckpointTask(CollectionCheckpointState &checkpoint_state)
	    : BaseExecutorTask(checkpoint_state.executor), checkpoint_state(checkpoint_state) {
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
		auto &row_group = *entry.node;
		checkpoint_state.writers[index] = checkpoint_state.writer.GetRowGroupWriter(*entry.node);
		checkpoint_state.write_data[index] = row_group.WriteToDisk(*checkpoint_state.writers[index]);
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
	           idx_t merge_count, idx_t target_count, idx_t merge_rows, idx_t row_start)
	    : BaseCheckpointTask(checkpoint_state), vacuum_state(vacuum_state), segment_idx(segment_idx),
	      merge_count(merge_count), target_count(target_count), merge_rows(merge_rows), row_start(row_start) {
	}

	void ExecuteTask() override {
		auto &collection = checkpoint_state.collection;
		const idx_t row_group_size = collection.GetRowGroupSize();
		auto &types = collection.GetTypes();
		// create the new set of target row groups (initially empty)
		vector<unique_ptr<RowGroup>> new_row_groups;
		vector<idx_t> append_counts;
		idx_t row_group_rows = merge_rows;
		idx_t start = row_start;
		for (idx_t target_idx = 0; target_idx < target_count; target_idx++) {
			idx_t current_row_group_rows = MinValue<idx_t>(row_group_rows, row_group_size);
			auto new_row_group = make_uniq<RowGroup>(collection, start, current_row_group_rows);
			new_row_group->InitializeEmpty(types);
			new_row_groups.push_back(std::move(new_row_group));
			append_counts.push_back(0);

			row_group_rows -= current_row_group_rows;
			start += current_row_group_rows;
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
		scan_state.table_state.Initialize(types);
		scan_state.table_state.max_row = idx_t(-1);
		idx_t merged_groups = 0;
		idx_t total_row_groups = vacuum_state.row_group_counts.size();
		for (idx_t c_idx = segment_idx; merged_groups < merge_count && c_idx < total_row_groups; c_idx++) {
			if (vacuum_state.row_group_counts[c_idx] == 0) {
				continue;
			}
			merged_groups++;

			auto &current_row_group = *checkpoint_state.segments[c_idx].node;

			current_row_group.InitializeScan(scan_state.table_state);
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
			checkpoint_state.segments[c_idx].node.reset();
		}
		idx_t total_append_count = 0;
		for (idx_t target_idx = 0; target_idx < target_count; target_idx++) {
			auto &row_group = new_row_groups[target_idx];
			row_group->Verify();

			// assign the new row group to the current segment
			checkpoint_state.segments[segment_idx + target_idx].node = std::move(row_group);
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

private:
	VacuumState &vacuum_state;
	idx_t segment_idx;
	idx_t merge_count;
	idx_t target_count;
	idx_t merge_rows;
	idx_t row_start;
};

void RowGroupCollection::InitializeVacuumState(CollectionCheckpointState &checkpoint_state, VacuumState &state,
                                               vector<SegmentNode<RowGroup>> &segments) {
	bool is_full_checkpoint = checkpoint_state.writer.GetCheckpointType() == CheckpointType::FULL_CHECKPOINT;
	// currently we can only vacuum deletes if we are doing a full checkpoint and there are no indexes
	state.can_vacuum_deletes = info->GetIndexes().Empty() && is_full_checkpoint;
	if (!state.can_vacuum_deletes) {
		return;
	}
	// obtain the set of committed row counts for each row group
	state.row_group_counts.reserve(segments.size());
	for (auto &entry : segments) {
		auto &row_group = *entry.node;
		auto row_group_count = row_group.GetCommittedRowCount();
		if (row_group_count == 0) {
			// empty row group - we can drop it entirely
			row_group.CommitDrop();
			entry.node.reset();
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
		D_ASSERT(!checkpoint_state.segments[segment_idx].node);
		return false;
	}
	if (!schedule_vacuum) {
		return false;
	}
	idx_t merge_rows;
	idx_t next_idx = 0;
	idx_t merge_count;
	idx_t target_count;
	bool perform_merge = false;
	// check if we can merge row groups adjacent to the current segment_idx
	// we try merging row groups into batches of 1-3 row groups
	// our goal is to reduce the amount of row groups
	// hence we target_count should be less than merge_count for a marge to be worth it
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
	auto vacuum_task = make_uniq<VacuumTask>(checkpoint_state, state, segment_idx, merge_count, target_count,
	                                         merge_rows, state.row_start);
	checkpoint_state.executor.ScheduleTask(std::move(vacuum_task));
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
	auto l = row_groups->Lock();
	auto segments = row_groups->MoveSegments(l);

	CollectionCheckpointState checkpoint_state(*this, writer, segments, global_stats);

	VacuumState vacuum_state;
	InitializeVacuumState(checkpoint_state, vacuum_state, segments);

	try {
		// schedule tasks
		idx_t total_vacuum_tasks = 0;
		auto &config = DBConfig::GetConfig(writer.GetDatabase());

		for (idx_t segment_idx = 0; segment_idx < segments.size(); segment_idx++) {
			auto &entry = segments[segment_idx];
			auto vacuum_tasks = ScheduleVacuumTasks(checkpoint_state, vacuum_state, segment_idx,
			                                        total_vacuum_tasks < config.options.max_vacuum_tasks);
			if (vacuum_tasks) {
				// vacuum tasks were scheduled - don't schedule a checkpoint task yet
				total_vacuum_tasks++;
				continue;
			}
			if (!entry.node) {
				// row group was vacuumed/dropped - skip
				continue;
			}
			// schedule a checkpoint task for this row group
			entry.node->MoveToCollection(*this, vacuum_state.row_start);
			auto checkpoint_task = GetCheckpointTask(checkpoint_state, segment_idx);
			checkpoint_state.executor.ScheduleTask(std::move(checkpoint_task));
			vacuum_state.row_start += entry.node->count;
		}
	} catch (const std::exception &e) {
		ErrorData error(e);
		checkpoint_state.executor.PushError(std::move(error));
		checkpoint_state.executor.WorkOnTasks(); // ensure all tasks have completed first before rethrowing
		throw;
	}
	// all tasks have been successfully scheduled - execute tasks until we are done
	checkpoint_state.executor.WorkOnTasks();

	// no errors - finalize the row groups
	idx_t new_total_rows = 0;
	for (idx_t segment_idx = 0; segment_idx < segments.size(); segment_idx++) {
		auto &entry = segments[segment_idx];
		if (!entry.node) {
			// row group was vacuumed/dropped - skip
			continue;
		}
		auto &row_group = *entry.node;
		auto row_group_writer = std::move(checkpoint_state.writers[segment_idx]);
		if (!row_group_writer) {
			throw InternalException("Missing row group writer for index %llu", segment_idx);
		}
		auto pointer =
		    row_group.Checkpoint(std::move(checkpoint_state.write_data[segment_idx]), *row_group_writer, global_stats);
		writer.AddRowGroup(std::move(pointer), std::move(row_group_writer));
		row_groups->AppendSegment(l, std::move(entry.node));
		new_total_rows += row_group.count;
	}
	total_rows = new_total_rows;
}

//===--------------------------------------------------------------------===//
// CommitDrop
//===--------------------------------------------------------------------===//
void RowGroupCollection::CommitDropColumn(const idx_t column_index) {
	for (auto &row_group : row_groups->Segments()) {
		row_group.CommitDropColumn(column_index);
	}
}

void RowGroupCollection::CommitDropTable() {
	for (auto &row_group : row_groups->Segments()) {
		row_group.CommitDrop();
	}
}

//===--------------------------------------------------------------------===//
// GetPartitionStats
//===--------------------------------------------------------------------===//
vector<PartitionStatistics> RowGroupCollection::GetPartitionStats() const {
	vector<PartitionStatistics> result;
	for (auto &row_group : row_groups->Segments()) {
		result.push_back(row_group.GetPartitionStats());
	}
	return result;
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
                                                             ExpressionExecutor &default_executor) {
	idx_t new_column_idx = types.size();
	auto new_types = types;
	new_types.push_back(new_column.GetType());
	auto result = make_shared_ptr<RowGroupCollection>(info, block_manager, std::move(new_types), row_start,
	                                                  total_rows.load(), row_group_size);

	DataChunk dummy_chunk;
	Vector default_vector(new_column.GetType());

	result->stats.InitializeAddColumn(stats, new_column.GetType());
	auto lock = result->stats.GetLock();
	auto &new_column_stats = result->stats.GetStats(*lock, new_column_idx);

	// fill the column with its DEFAULT value, or NULL if none is specified
	auto new_stats = make_uniq<SegmentStatistics>(new_column.GetType());
	for (auto &current_row_group : row_groups->Segments()) {
		auto new_row_group = current_row_group.AddColumn(*result, new_column, default_executor, default_vector);
		// merge in the statistics
		new_row_group->MergeIntoStatistics(new_column_idx, new_column_stats.Statistics());

		result->row_groups->AppendSegment(std::move(new_row_group));
	}

	return result;
}

shared_ptr<RowGroupCollection> RowGroupCollection::RemoveColumn(idx_t col_idx) {
	D_ASSERT(col_idx < types.size());
	auto new_types = types;
	new_types.erase_at(col_idx);

	auto result = make_shared_ptr<RowGroupCollection>(info, block_manager, std::move(new_types), row_start,
	                                                  total_rows.load(), row_group_size);
	result->stats.InitializeRemoveColumn(stats, col_idx);

	auto result_lock = result->stats.GetLock();
	result->stats.DestroyTableSample(*result_lock);

	for (auto &current_row_group : row_groups->Segments()) {
		auto new_row_group = current_row_group.RemoveColumn(*result, col_idx);
		result->row_groups->AppendSegment(std::move(new_row_group));
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

	auto result = make_shared_ptr<RowGroupCollection>(info, block_manager, std::move(new_types), row_start,
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
	scan_state.table_state.max_row = row_start + total_rows;

	// now alter the type of the column within all of the row_groups individually
	auto lock = result->stats.GetLock();
	auto &changed_stats = result->stats.GetStats(*lock, changed_idx);
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
	InitializeScan(state.table_state, column_ids, nullptr);

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
