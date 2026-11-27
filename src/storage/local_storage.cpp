#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/transaction/commit_state.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

LocalTableStorage::LocalTableStorage(Context &context, DataTable &table)
    : table_ref(table), allocator(Allocator::Get(table.db)), deleted_rows(0), optimistic_writer(table),
      indexes(make_shared_ptr<LocalStorageIndexes>()) {
	auto types = table.GetTypes();
	auto data_table_info = table.GetDataTableInfo();
	row_groups = optimistic_writer.CreateCollection(context, table, types, OptimisticWritePartialManagers::GLOBAL);
	auto &collection = *row_groups->collection;
	collection.InitializeEmpty();

	// Create the transaction-local delete and append indexes.
	data_table_info->GetIndexes().InitializeLocalIndexes(GetDeleteIndexes(), GetAppendIndexes());
}

//! The storage of the old version of the table stays as it is: it shares the indexes and the persistent column
//! data with the new version, and is not used for commit or rollback anymore
static void PrepareParentForAlter(ClientContext &context, LocalTableStorage &parent) {
	if (parent.HasOptimisticCollections()) {
		throw TransactionException("Cannot alter a table that is being written to by another statement");
	}
	// finish the partially written blocks: the new version does not take over the writer
	parent.GetOptimisticWriter().FinalFlush(context.GetContext());
}

static unique_ptr<OptimisticWriteCollection> CreateAlteredCollection(shared_ptr<RowGroupCollection> collection) {
	auto result = make_uniq<OptimisticWriteCollection>();
	result->collection = std::move(collection);
	return result;
}

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &new_data_table, LocalTableStorage &parent,
                                     const idx_t alter_column_index, const LogicalType &target_type,
                                     const vector<StorageIndex> &bound_columns, Expression &cast_expr,
                                     TransactionData transaction)
    : table_ref(new_data_table), allocator(Allocator::Get(new_data_table.db)), deleted_rows(parent.deleted_rows.load()),
      optimistic_writer(new_data_table), indexes(parent.indexes) {
	PrepareParentForAlter(context, parent);
	// Alter the column type.
	auto &parent_collection = *parent.row_groups->collection;
	auto new_collection =
	    parent_collection.AlterType(context, alter_column_index, target_type, bound_columns, cast_expr, transaction);
	parent_collection.CommitDropColumn(alter_column_index);
	row_groups = CreateAlteredCollection(std::move(new_collection));
}

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &new_data_table, LocalTableStorage &parent,
                                     const idx_t drop_column_index)
    : table_ref(new_data_table), allocator(Allocator::Get(new_data_table.db)), deleted_rows(parent.deleted_rows.load()),
      optimistic_writer(new_data_table), indexes(parent.indexes) {
	PrepareParentForAlter(context, parent);
	// Remove the column from the previous table storage.
	auto &parent_collection = *parent.row_groups->collection;
	auto new_collection = parent_collection.RemoveColumn(drop_column_index);
	parent_collection.CommitDropColumn(drop_column_index);
	row_groups = CreateAlteredCollection(std::move(new_collection));
}

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &new_dt, LocalTableStorage &parent,
                                     ColumnDefinition &new_column, ExpressionExecutor &default_executor)
    : table_ref(new_dt), allocator(Allocator::Get(new_dt.db)), deleted_rows(parent.deleted_rows.load()),
      optimistic_writer(new_dt), indexes(parent.indexes) {
	PrepareParentForAlter(context, parent);
	auto &parent_collection = *parent.row_groups->collection;
	auto new_collection = parent_collection.AddColumn(context, new_column, default_executor);
	row_groups = CreateAlteredCollection(std::move(new_collection));
}

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &new_dt, LocalTableStorage &parent)
    : table_ref(new_dt), allocator(Allocator::Get(new_dt.db)), deleted_rows(parent.deleted_rows.load()),
      optimistic_writer(new_dt), indexes(parent.indexes) {
	// a new constraint does not change the data: the row groups are shared with the old version
	PrepareParentForAlter(context, parent);
	row_groups = CreateAlteredCollection(parent.row_groups->collection);
}

LocalTableStorage::~LocalTableStorage() {
}

void LocalTableStorage::InitializeScan(Context &scan_context, CollectionScanState &state,
                                       optional_ptr<TableFilterSet> table_filters) {
	auto &collection = *row_groups->collection;
	if (collection.GetTotalRows() == 0) {
		throw InternalException("No rows in LocalTableStorage row group for scan");
	}
	collection.InitializeScan(scan_context, state, state.GetColumnIds(), table_filters.get());
}

idx_t LocalTableStorage::EstimatedSize() const {
	// count the appended rows
	auto &collection = *row_groups->collection;
	idx_t data_size = 0;

	if (collection.GetTotalRows() >= collection.GetRowGroupSize() && deleted_rows == 0) {
		// Optimistic insertion does not generate many WAL logs, so we estimate the size of the Data Block Pointers here
		idx_t row_group_count = row_groups->collection->GetRowGroupCount();
		idx_t column_count = collection.GetTypes().size();

		data_size = row_group_count * (sizeof(PersistentRowGroupData) +
		                               column_count * (sizeof(PersistentColumnData) + sizeof(DataPointer)));
	} else {
		idx_t appended_rows = collection.GetTotalRows() - deleted_rows;

		// get the (estimated) size of a row (no compressions, etc.)
		idx_t row_size = 0;
		auto &types = collection.GetTypes();
		for (auto &type : types) {
			row_size += GetTypeIdSize(type.InternalType());
		}

		data_size = appended_rows * row_size;
	}

	// return the size of the appended rows and the index size
	return data_size + GetAppendIndexes().GetInMemorySize();
}

void LocalTableStorage::WriteNewRowGroup(Context &context, idx_t flushed_row_group_idx) {
	// a flush replaces row groups: skip it while they are in use, the row groups are written at the latest on commit
	auto flush_key = flush_lock.TryGetExclusiveLock();
	if (!flush_key) {
		return;
	}
	if (deleted_rows != 0) {
		// we have deletes - we cannot merge row groups
		return;
	}
	optimistic_writer.WriteNewRowGroup(context, *row_groups, flushed_row_group_idx);
}

void LocalTableStorage::FlushBlocks(Context &context) {
	auto &collection = *row_groups->collection;
	const idx_t row_group_size = collection.GetRowGroupSize();
	if (collection.GetTotalRows() >= row_group_size) {
		// write any unflushed row groups
		optimistic_writer.WriteUnflushedRowGroups(context, *row_groups);
	}
	optimistic_writer.FinalFlush(context);
}

bool LocalTableStorage::WritesToDisk() const {
	return optimistic_writer.CanWriteToDisk();
}

bool LocalTableStorage::HasFlushedRowGroups() const {
	return !row_groups->flushed_row_groups.empty();
}

bool LocalTableStorage::IsBulkAppend() const {
	if (is_dropped || deleted_rows != 0) {
		return false;
	}
	auto &collection = *row_groups->collection;
	return collection.GetTotalRows() >= collection.GetRowGroupSize();
}

ErrorData LocalTableStorage::AppendToIndexes(DuckTransaction &transaction, RowGroupCollection &source,
                                             TableIndexList &index_list, const vector<LogicalType> &table_types,
                                             row_t &start_row) {
	// We only scan the indexed columns: mapped_column_ids lists them (sorted for a deterministic order),
	// and the scan below produces index_chunk in that order, i.e., index_chunk.data[i] holds the data of
	// the table's physical column mapped_column_ids[i].
	D_ASSERT(!index_list.Empty());
	auto indexed_columns = index_list.GetIndexedColumns();
	vector<StorageIndex> mapped_column_ids;
	for (auto &col : indexed_columns) {
		mapped_column_ids.emplace_back(col);
	}
	std::sort(mapped_column_ids.begin(), mapped_column_ids.end());
	auto checkpoint_id = transaction.GetTransactionManager().Cast<DuckTransactionManager>().GetActiveCheckpoint();

	// The bound expressions of the indexes (and their bound column references) are in relation to
	// ALL table columns, so we create an empty table chunk based on the table types. It references
	// the indexed columns, and contains nothing for all non-indexed columns.
	DataChunk table_chunk;
	table_chunk.InitializeEmpty(table_types);

	ErrorData error;
	for (auto &index_chunk : source.Chunks(transaction, mapped_column_ids)) {
		D_ASSERT(index_chunk.ColumnCount() == mapped_column_ids.size());
		for (idx_t i = 0; i < mapped_column_ids.size(); i++) {
			auto col_id = mapped_column_ids[i].GetPrimaryIndex();
			table_chunk.data[col_id].Reference(index_chunk.data[i]);
		}

		error = index_list.Append(GetDeleteIndexes(), table_chunk, start_row, index_append_mode, checkpoint_id);
		if (error.HasError()) {
			break;
		}
		start_row += UnsafeNumericCast<row_t>(index_chunk.size());
	}
	return error;
}

ErrorData LocalTableStorage::AppendToTable(DuckTransaction &transaction, TableAppendState &append_state) {
	auto &table = table_ref.get();
	const auto index_end = append_state.current_row;
	try {
		table.InitializeAppend(transaction, append_state);
		auto &collection = *row_groups->collection;
		for (auto &table_chunk : collection.Chunks(transaction)) {
			// Append to the base table.
			table.Append(table_chunk, append_state);
		}
		table.FinalizeAppend(transaction, append_state);
	} catch (std::exception &ex) {
		// remove the rows and index entries that the failed append added
		table.RevertFailedAppend(append_state);
		// free the memory of the reverted rows before this storage is scanned again
		append_state.row_group_append_state.states.reset();
		append_state.row_groups.reset();
		auto revert_error = RevertAppendToIndexes(transaction, append_state.row_start, index_end);
		if (revert_error.HasError()) {
			revert_error.Throw();
		}
		return ErrorData(ex);
	}
	return ErrorData();
}

ErrorData LocalTableStorage::AppendToIndexes(DuckTransaction &transaction, TableAppendState &append_state) {
	// In this function, we might scan all table columns,
	// as we might also append to the table itself (append_to_table).
	auto &table = table_ref.get();
	if (!table.HasIndexes()) {
		// no indexes to append to
		return ErrorData();
	}
	auto data_table_info = table.GetDataTableInfo();
	auto &index_list = data_table_info->GetIndexes();
	auto &collection = *row_groups->collection;
	auto error = AppendToIndexes(transaction, collection, index_list, table.GetTypes(), append_state.current_row);
	if (error.HasError()) {
		// the failing chunk's keys were already removed; revert the chunks appended before it
		auto revert_error = RevertAppendToIndexes(transaction, append_state.row_start, append_state.current_row);
		if (revert_error.HasError()) {
			revert_error.Throw();
		}
	}
	return error;
}

ErrorData LocalTableStorage::RevertAppendToIndexes(DuckTransaction &transaction, row_t row_start, row_t row_end) {
	auto &table = table_ref.get();
	if (!table.HasIndexes()) {
		return ErrorData();
	}
	auto &index_list = table.GetDataTableInfo()->GetIndexes();
	auto &collection = *row_groups->collection;
	ErrorData error;
	row_t current_row = row_start;
	// Remove the data from the indexes, if any.
	for (auto &chunk : collection.Chunks(transaction)) {
		if (current_row >= row_end) {
			// Finished deleting all rows from the index.
			break;
		}
		// Remove the chunk.
		try {
			index_list.RevertAppend(chunk, current_row);
		} catch (std::exception &ex) { // LCOV_EXCL_START
			error = ErrorData(ex);
			break;
		} // LCOV_EXCL_STOP

		current_row += UnsafeNumericCast<row_t>(chunk.size());
	}

#ifdef DEBUG
	// Verify that our index memory is stable.
	table.VerifyIndexBuffers();
#endif
	return error;
}

PhysicalIndex LocalTableStorage::CreateOptimisticCollection(unique_ptr<OptimisticWriteCollection> collection) {
	lock_guard<mutex> l(collections_lock);
	optimistic_collections.push_back(std::move(collection));
	return PhysicalIndex(optimistic_collections.size() - 1);
}

OptimisticWriteCollection &LocalTableStorage::GetOptimisticCollection(const PhysicalIndex collection_index) {
	lock_guard<mutex> l(collections_lock);
	if (collection_index.index >= optimistic_collections.size() || !optimistic_collections[collection_index.index]) {
		// the table was altered by a connection that shares the transaction while the statement was running
		throw TransactionException("The table was modified concurrently by a connection sharing the transaction");
	}
	return *optimistic_collections[collection_index.index];
}

void LocalTableStorage::ResetOptimisticCollection(const PhysicalIndex collection_index) {
	lock_guard<mutex> l(collections_lock);
	if (collection_index.index >= optimistic_collections.size()) {
		throw TransactionException("The table was modified concurrently by a connection sharing the transaction");
	}
	optimistic_collections[collection_index.index].reset();
}

unique_ptr<StorageLockKey> LocalTableStorage::LockShared() {
	auto key = flush_lock.GetSharedLock();
	if (replaced_by_alter) {
		throw TransactionException("The table was altered concurrently by a connection sharing the transaction");
	}
	return key;
}

unique_ptr<StorageLockKey> LocalTableStorage::LockAppend() {
	auto key = append_lock.GetExclusiveLock();
	if (replaced_by_alter) {
		throw TransactionException("The table was altered concurrently by a connection sharing the transaction");
	}
	return key;
}

bool LocalTableStorage::HasOptimisticCollections() {
	lock_guard<mutex> l(collections_lock);
	for (auto &collection : optimistic_collections) {
		if (collection) {
			return true;
		}
	}
	return false;
}

OptimisticDataWriter &LocalTableStorage::GetOptimisticWriter() {
	return optimistic_writer;
}

void LocalTableStorage::Rollback() {
	optimistic_writer.Rollback();

	CommitDropState drop_state(&row_groups->collection->GetBlockManager());
	for (auto &collection : optimistic_collections) {
		if (!collection) {
			continue;
		}
		collection->collection->CommitDropTable(drop_state);
	}
	optimistic_collections.clear();
	row_groups->collection->CommitDropTable(drop_state);
	drop_state.FinalizeCommit();
}

//===--------------------------------------------------------------------===//
// LocalTableManager
//===--------------------------------------------------------------------===//
shared_ptr<LocalTableStorage> LocalTableManager::GetStorage(const DataTable &table) const {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	return entry == table_storage.end() ? nullptr : entry->second;
}

shared_ptr<LocalTableStorage> LocalTableManager::GetOrCreateStorage(Context &context, DataTable &table) {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		if (altered_tables.find(table) != altered_tables.end()) {
			throw TransactionException("The table was altered concurrently by a connection sharing the transaction");
		}
		auto new_storage = make_shared_ptr<LocalTableStorage>(context, table);
		table_storage.insert(make_pair(reference<const DataTable>(table), new_storage));
		return new_storage;
	}
	return entry->second;
}

bool LocalTableManager::IsEmpty() const {
	lock_guard<mutex> l(table_storage_lock);
	return table_storage.empty();
}

shared_ptr<LocalTableStorage> LocalTableManager::MoveEntry(DataTable &table) {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		return nullptr;
	}
	auto storage_entry = std::move(entry->second);
	table_storage.erase(entry);
	return storage_entry;
}

void LocalTableManager::ReplaceEntry(DataTable &old_table, DataTable &new_table, shared_ptr<LocalTableStorage> entry) {
	lock_guard<mutex> l(table_storage_lock);
	auto old_entry = table_storage.find(old_table);
	D_ASSERT(old_entry != table_storage.end());
	old_entry->second->replaced_by_alter = true;
	table_storage.erase(old_entry);
	altered_tables.insert(old_table);
	D_ASSERT(table_storage.find(new_table) == table_storage.end());
	table_storage[new_table] = std::move(entry);
}

reference_map_t<const DataTable, shared_ptr<LocalTableStorage>> LocalTableManager::MoveEntries() {
	lock_guard<mutex> l(table_storage_lock);
	return std::move(table_storage);
}

vector<shared_ptr<LocalTableStorage>> LocalTableManager::GetEntries() const {
	lock_guard<mutex> l(table_storage_lock);
	vector<shared_ptr<LocalTableStorage>> result;
	result.reserve(table_storage.size());
	for (auto &entry : table_storage) {
		result.push_back(entry.second);
	}
	return result;
}

idx_t LocalTableManager::EstimatedSize() const {
	lock_guard<mutex> l(table_storage_lock);
	idx_t estimated_size = 0;
	for (auto &storage : table_storage) {
		estimated_size += storage.second->EstimatedSize();
	}
	return estimated_size;
}

void LocalTableManager::InsertEntry(DataTable &table, shared_ptr<LocalTableStorage> entry) {
	lock_guard<mutex> l(table_storage_lock);
	D_ASSERT(table_storage.find(table) == table_storage.end());
	table_storage[table] = std::move(entry);
}

//===--------------------------------------------------------------------===//
// LocalStorage
//===--------------------------------------------------------------------===//
LocalStorage::LocalStorage(DuckTransaction &transaction) : transaction(transaction) {
}

LocalStorage::CommitState::CommitState() {
}

LocalStorage::CommitState::~CommitState() {
}

LocalStorage &LocalStorage::Get(DuckTransaction &transaction) {
	return transaction.GetLocalStorage();
}

LocalStorage &LocalStorage::Get(ClientContext &context, AttachedDatabase &db) {
	return DuckTransaction::Get(context, db).GetLocalStorage();
}

LocalStorage &LocalStorage::Get(ClientContext &context, Catalog &catalog) {
	return LocalStorage::Get(context, catalog.GetAttached());
}

void LocalStorage::InitializeScan(Context &context, DataTable &table, CollectionScanState &state,
                                  optional_ptr<TableFilterSet> table_filters) {
	auto storage = table_manager.GetStorage(table);
	if (storage == nullptr) {
		return;
	}
	state.local_storage = storage;
	auto row_groups_lock = storage->LockShared();
	if (storage->GetCollection().GetTotalRows() == 0) {
		return;
	}
	state.local_row_groups_lock = std::move(row_groups_lock);
	storage->InitializeScan(context, state, table_filters);
}

void LocalStorage::Scan(CollectionScanState &state, const vector<StorageIndex> &, DataChunk &result) {
	state.Scan(transaction, result);
}

void LocalStorage::InitializeParallelScan(DataTable &table, ParallelCollectionScanState &state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		state.max_row = 0;
		state.vector_index = 0;
		state.current_row_group = nullptr;
	} else {
		auto flush_key = storage->LockShared();
		storage->GetCollection().InitializeParallelScan(state);
	}
}

RowGroupCollection &LocalTableStorage::GetCollection() {
	return *row_groups->collection;
}

OptimisticWriteCollection &LocalTableStorage::GetPrimaryCollection() {
	return *row_groups;
}

optional_idx LocalStorage::NextParallelScan(ClientContext &context, DataTable &table,
                                            ParallelCollectionScanState &state, CollectionScanState &scan_state,
                                            bool initialize_columns) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return optional_idx();
	}
	if (!scan_state.local_row_groups_lock) {
		scan_state.local_storage = storage;
		scan_state.local_row_groups_lock = storage->LockShared();
	}
	return storage->GetCollection().NextParallelScan(context, state, scan_state, initialize_columns);
}

void LocalStorage::InitializeAppend(Context &context, LocalAppendState &state, DataTable &table,
                                    DuckTableEntry &table_entry) {
	state.context = &context;
	state.storage = table_manager.GetOrCreateStorage(context, table);
	state.append_lock = state.storage->LockAppend();
	state.storage->table_entry = &table_entry;
	state.storage->GetCollection().InitializeAppend(TransactionData(transaction), state.append_state);
}

void LocalStorage::InitializeStorage(Context &context, LocalAppendState &state, DataTable &table,
                                     DuckTableEntry &table_entry) {
	state.context = &context;
	state.storage = table_manager.GetOrCreateStorage(context, table);
	state.storage->table_entry = &table_entry;
}

void LocalTableStorage::AppendToDeleteIndexes(Vector &row_ids, DataChunk &delete_chunk) {
	if (delete_chunk.size() == 0) {
		return;
	}

	// Only committed row IDs (< MAX_ROW_ID) belong in the delete indexes.
	// Local row IDs (>= MAX_ROW_ID) live in transaction-local storage and are
	// handled directly by LocalStorage::Delete.
	row_ids.Flatten();
	auto flat_row_ids = FlatVector::GetData<row_t>(row_ids);
	idx_t committed_count = 0;
	SelectionVector committed_sel(delete_chunk.size());
	for (idx_t i = 0; i < delete_chunk.size(); i++) {
		if (flat_row_ids[i] < MAX_ROW_ID) {
			committed_sel.set_index(committed_count++, i);
		}
	}

	if (committed_count == 0) {
		return;
	}
	if (committed_count == delete_chunk.size()) {
		GetDeleteIndexes().AppendToDeleteIndexes(delete_chunk, row_ids);
		return;
	}

	DataChunk committed_chunk;
	committed_chunk.InitializeEmpty(delete_chunk.GetTypes());
	committed_chunk.Slice(delete_chunk, committed_sel, committed_count);

	Vector committed_row_ids(row_ids, committed_sel, committed_count);
	committed_row_ids.Flatten();

	GetDeleteIndexes().AppendToDeleteIndexes(committed_chunk, committed_row_ids);
}

void LocalStorage::Append(LocalAppendState &state, DuckTableEntry &table_entry, DataChunk &table_chunk) {
	auto storage = state.storage;
	storage->table_entry = &table_entry;
	auto offset = NumericCast<idx_t>(MAX_ROW_ID) + storage->GetCollection().GetNextRowId();
	idx_t base_id = offset + state.append_state.total_append_count;

	if (!storage->GetAppendIndexes().Empty()) {
		auto error =
		    storage->GetAppendIndexes().Append(storage->GetDeleteIndexes(), table_chunk, NumericCast<row_t>(base_id),
		                                       storage->index_append_mode, optional_idx());
		if (error.HasError()) {
			error.Throw();
		}
	}

	// Append the chunk to the local storage.
	auto flushed_row_group_idx = storage->GetCollection().Append(table_chunk, state.append_state);

	// Check if we should pre-emptively flush blocks to disk.
	if (flushed_row_group_idx.IsValid()) {
		storage->WriteNewRowGroup(*state.context, flushed_row_group_idx.GetIndex());
	}
}

void LocalStorage::FinalizeAppend(LocalAppendState &state) {
	state.storage->GetCollection().FinalizeAppend(state.append_state.transaction, state.append_state);
	state.append_lock.reset();
}

void LocalStorage::LocalMerge(Context &context, DataTable &table, DuckTableEntry &table_entry,
                              OptimisticWriteCollection &collection) {
	auto storage_ptr = table_manager.GetOrCreateStorage(context, table);
	auto &storage = *storage_ptr;
	auto append_lock = storage.LockAppend();
	storage.table_entry = &table_entry;
	if (!storage.GetAppendIndexes().Empty()) {
		// append data to indexes if required
		row_t base_id = MAX_ROW_ID + NumericCast<row_t>(storage.GetCollection().GetNextRowId());
		auto error = storage.AppendToIndexes(transaction, *collection.collection, storage.GetAppendIndexes(),
		                                     table.GetTypes(), base_id);
		if (error.HasError()) {
			error.Throw();
		}
	}
	auto &target = storage.GetPrimaryCollection();
	target.MergeStorage(collection);
}

PhysicalIndex LocalStorage::CreateOptimisticCollection(Context &context, DataTable &table,
                                                       unique_ptr<OptimisticWriteCollection> collection) {
	auto storage_ptr = table_manager.GetOrCreateStorage(context, table);
	auto &storage = *storage_ptr;
	return storage.CreateOptimisticCollection(std::move(collection));
}

OptimisticWriteCollection &LocalStorage::GetOptimisticCollection(Context &context, DataTable &table,
                                                                 const PhysicalIndex collection_index) {
	auto storage_ptr = table_manager.GetOrCreateStorage(context, table);
	auto &storage = *storage_ptr;
	return storage.GetOptimisticCollection(collection_index);
}

void LocalStorage::ResetOptimisticCollection(Context &context, DataTable &table, const PhysicalIndex collection_index) {
	auto storage_ptr = table_manager.GetOrCreateStorage(context, table);
	auto &storage = *storage_ptr;
	storage.ResetOptimisticCollection(collection_index);
}

OptimisticDataWriter &LocalStorage::GetOptimisticWriter(Context &context, DataTable &table) {
	auto storage_ptr = table_manager.GetOrCreateStorage(context, table);
	auto &storage = *storage_ptr;
	return storage.GetOptimisticWriter();
}

bool LocalStorage::ChangesMade() noexcept {
	return !table_manager.IsEmpty();
}

bool LocalStorage::Find(DataTable &table) {
	return table_manager.GetStorage(table) != nullptr;
}

idx_t LocalStorage::EstimatedSize() {
	return table_manager.EstimatedSize();
}

idx_t LocalStorage::Delete(Context &context, DataTable &table, DuckTableEntry &table_entry, Vector &row_ids,
                           idx_t count) {
	auto storage = table_manager.GetStorage(table);
	D_ASSERT(storage);

	auto flush_key = storage->LockShared();
	auto ids = FlatVector::GetDataMutable<row_t>(row_ids);
	if (storage->GetAppendIndexes().Empty()) {
		idx_t delete_count = storage->GetCollection().Delete(TransactionData::Unversioned(), table_entry, ids, count);
		storage->deleted_rows += delete_count;
		return delete_count;
	}
	// a row can be matched multiple times (e.g. DELETE ... USING) - only remove the rows that were deleted now from
	// the unique indices
	DeletedRowIds deleted_ids(ids);
	idx_t delete_count =
	    storage->GetCollection().Delete(TransactionData::Unversioned(), table_entry, ids, count, deleted_ids);
	storage->deleted_rows += delete_count;
	if (!deleted_ids.has_skipped_rows) {
		// all rows were deleted
		storage->GetCollection().RemoveFromIndexes(context, storage->GetAppendIndexes(), row_ids, count,
		                                           IndexRemovalType::MAIN_INDEX_ONLY);
		return delete_count;
	}
	auto &deleted_row_ids = deleted_ids.row_ids;
	if (!deleted_row_ids.empty()) {
		Vector deleted_row_id_vector(LogicalType::ROW_TYPE, deleted_row_ids.size());
		{
			auto writer = FlatVector::Writer<row_t>(deleted_row_id_vector, deleted_row_ids.size());
			for (auto &row_id : deleted_row_ids) {
				writer.WriteValue(row_id);
			}
		}
		storage->GetCollection().RemoveFromIndexes(context, storage->GetAppendIndexes(), deleted_row_id_vector,
		                                           deleted_row_ids.size(), IndexRemovalType::MAIN_INDEX_ONLY);
	}
	return delete_count;
}

void LocalStorage::Update(DataTable &table, DuckTableEntry &table_entry, Vector &row_ids,
                          const vector<PhysicalIndex> &column_ids, DataChunk &updates) {
	D_ASSERT(updates.size() >= 1);
	auto storage = table_manager.GetStorage(table);
	D_ASSERT(storage);

	auto flush_key = storage->LockShared();
	auto ids = FlatVector::GetDataMutable<row_t>(row_ids);
	storage->GetCollection().Update(TransactionData::Unversioned(), table_entry, ids, column_ids, updates);
}

void LocalStorage::Flush(Context &context, DataTable &table, LocalTableStorage &storage,
                         optional_ptr<StorageCommitState> commit_state) {
	if (storage.is_dropped) {
		storage.Rollback();
		return;
	}
	if (storage.GetCollection().GetTotalRows() <= storage.deleted_rows) {
		// all rows that we added were deleted
		// rollback any partial blocks that are still outstanding
		storage.Rollback();
		return;
	}

	auto append_count = storage.GetCollection().GetTotalRows() - storage.deleted_rows;

	TableAppendState append_state;
	table.AppendLock(transaction, append_state);
	// until PushAppend, a returned error means the shared state was restored; anything thrown invalidates
	ErrorData error;
	try {
		if (storage.IsBulkAppend() ||
		    (append_state.row_start == 0 && storage.deleted_rows == 0 && !storage.WritesToDisk())) {
			// bulk append (at least one full row group, no deletes): move over the storage directly.
			// Appends to an empty table are also merged directly if the table cannot be written to
			// disk (temporary / in-memory / read-only, e.g. WAL replay of a read-only attach) -
			// there are no optimistically written blocks to manage, and merging avoids re-appending
			// row by row.
			// first flush any outstanding blocks
			storage.FlushBlocks(context);
			// Append to the indexes.
			error = storage.AppendToIndexes(transaction, append_state);
			if (!error.HasError()) {
				// finally move over the row groups
				table.MergeStorage(storage.GetCollection(), commit_state);
			}
		} else {
			// check if we have written data
			// if we have, we cannot merge to disk after all
			// so we need to revert the data we have already written
			// this happens when rows were deleted after a bulk append, or when the optimistic writer
			// flushed a partial row group that does not qualify as a bulk append
			storage.Rollback();
			// append to the indexes
			error = storage.AppendToIndexes(transaction, append_state);
			if (!error.HasError()) {
				// after that is successful - append to the table
				error = storage.AppendToTable(transaction, append_state);
			}
		}
		if (!error.HasError()) {
			if (!table.db.IsSystem() && !table.db.IsTemporary() &&
			    Settings::Get<DebugForceFlushAppendFailureSetting>(table.db.GetDatabase())) {
				throw IOException("Forced flush append failure (debug_force_flush_append_failure)");
			}
			// table_entry is set through the append path (InitializeAppend/Append/LocalMerge/Alter)
			auto table_entry = storage.table_entry.load();
			D_ASSERT(table_entry);
			transaction.PushAppend(*table_entry, NumericCast<idx_t>(append_state.row_start), append_count);
		}
	} catch (std::exception &ex) {
		// the shared table or indexes may be partially modified: no checkpoint may persist them
		table.db.Invalidate("flushing table \"" + table.GetTableName() + "\" during commit failed",
		                    ErrorData(ex).RawMessage());
		throw;
	}
	if (error.HasError()) {
		error.Throw();
	}

#ifdef DEBUG
	// Verify that our index memory is stable.
	table.VerifyIndexBuffers();
#endif
}

void LocalStorage::FlushBulkAppendBlocksAndSync(Context &context, AttachedDatabase &db) {
	bool requires_sync = false;
	for (auto &storage : table_manager.GetEntries()) {
		if (storage->IsBulkAppend()) {
			// Flush() is guaranteed to take the bulk path - the blocks will be used as written
			storage->FlushBlocks(context);
			requires_sync |= storage->HasFlushedRowGroups();
		}
	}
	if (requires_sync) {
		// the WAL will reference the flushed row groups (flushed just now or already during the
		// statement, e.g. by batch inserts) - persist them now so that the commit does not have
		// to FileSync while holding the commit lock
		db.GetStorageManager().GetBlockManager().FileSync();
		synced_flushed_blocks = true;
	}
}

void LocalStorage::Commit(Context &context, optional_ptr<StorageCommitState> commit_state) {
	// commit local storage
	// iterate over all entries in the table storage map and commit them
	// after this, the local storage is no longer required and can be cleared
	auto table_storage = table_manager.MoveEntries();
	for (auto &entry : table_storage) {
		auto storage = entry.second.get();
		D_ASSERT(RefersToSameObject(entry.first, storage->table_ref));
		Flush(context, storage->table_ref, *storage, commit_state);
		entry.second.reset();
	}
}

void LocalStorage::Rollback() {
	// rollback local storage
	// after this, the local storage is no longer required and can be cleared
	auto table_storage = table_manager.MoveEntries();
	for (auto &entry : table_storage) {
		auto storage = entry.second.get();
		if (!storage) {
			continue;
		}
		storage->Rollback();
		entry.second.reset();
	}
}

idx_t LocalStorage::AddedRows(DataTable &table) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return 0;
	}
	auto flush_key = storage->LockShared();
	return storage->GetCollection().GetTotalRows() - storage->deleted_rows;
}

vector<PartitionStatistics> LocalStorage::GetPartitionStats(DataTable &table, TransactionData transaction) const {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return vector<PartitionStatistics>();
	}
	auto flush_key = storage->LockShared();
	return storage->GetCollection().GetPartitionStats(transaction);
}

void LocalStorage::DropTable(DataTable &table) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return;
	}
	storage->is_dropped = true;
}

void LocalStorage::MoveStorage(ClientContext &context, DataTable &old_dt, DataTable &new_dt) {
	// check if there are any pending appends for the old version of the table
	LocalStorageAlterLocks locks;
	auto storage = BeginAlter(old_dt, locks);
	if (!storage) {
		return;
	}
	FinishAlter(old_dt, new_dt, make_shared_ptr<LocalTableStorage>(context, new_dt, *storage));
}

shared_ptr<LocalTableStorage> LocalStorage::BeginAlter(DataTable &old_dt, LocalStorageAlterLocks &locks) {
	auto storage = table_manager.GetStorage(old_dt);
	if (!storage) {
		return nullptr;
	}
	// statements of other connections that share the transaction can be using the storage
	locks.append_key = storage->append_lock.TryGetExclusiveLock();
	locks.flush_key = storage->flush_lock.TryGetExclusiveLock();
	if (!locks.append_key || !locks.flush_key) {
		throw TransactionException("Cannot alter a table that is used by another connection of the shared transaction");
	}
	return storage;
}

void LocalStorage::FinishAlter(DataTable &old_dt, DataTable &new_dt, shared_ptr<LocalTableStorage> new_storage) {
	// statements that wait for the locks of the old storage see that it was replaced
	table_manager.ReplaceEntry(old_dt, new_dt, std::move(new_storage));
}

void LocalStorage::AddColumn(ClientContext &context, DataTable &old_dt, DataTable &new_dt, ColumnDefinition &new_column,
                             ExpressionExecutor &default_executor) {
	// check if there are any pending appends for the old version of the table
	LocalStorageAlterLocks locks;
	auto storage = BeginAlter(old_dt, locks);
	if (!storage) {
		return;
	}
	FinishAlter(old_dt, new_dt,
	            make_shared_ptr<LocalTableStorage>(context, new_dt, *storage, new_column, default_executor));
}

void LocalStorage::DropColumn(ClientContext &context, DataTable &old_dt, DataTable &new_dt,
                              const idx_t drop_column_index) {
	// check if there are any pending appends for the old version of the table
	LocalStorageAlterLocks locks;
	auto storage = BeginAlter(old_dt, locks);
	if (!storage) {
		return;
	}
	FinishAlter(old_dt, new_dt, make_shared_ptr<LocalTableStorage>(context, new_dt, *storage, drop_column_index));
}

void LocalStorage::ChangeType(ClientContext &context, DataTable &old_dt, DataTable &new_dt, idx_t changed_idx,
                              const LogicalType &target_type, const vector<StorageIndex> &bound_columns,
                              Expression &cast_expr) {
	// check if there are any pending appends for the old version of the table
	LocalStorageAlterLocks locks;
	auto storage = BeginAlter(old_dt, locks);
	if (!storage) {
		return;
	}
	FinishAlter(old_dt, new_dt,
	            make_shared_ptr<LocalTableStorage>(context, new_dt, *storage, changed_idx, target_type, bound_columns,
	                                               cast_expr, transaction));
}

void LocalStorage::FetchChunk(DataTable &table, const Vector &row_ids, idx_t count, const vector<StorageIndex> &col_ids,
                              DataChunk &chunk, ColumnFetchState &fetch_state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		throw InternalException("LocalStorage::FetchChunk - local storage not found");
	}
	auto flush_key = storage->LockShared();
	storage->GetCollection().Fetch(transaction, chunk, col_ids, row_ids, count, fetch_state);
}

bool LocalStorage::CanFetch(DataTable &table, const row_t row_id) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		throw InternalException("LocalStorage::CanFetch - local storage not found");
	}
	auto flush_key = storage->LockShared();
	return storage->GetCollection().CanFetch(transaction, row_id);
}

TableIndexList &LocalStorage::GetIndexes(Context &context, DataTable &table) {
	auto storage_ptr = table_manager.GetOrCreateStorage(context, table);
	auto &storage = *storage_ptr;
	return storage.GetAppendIndexes();
}

shared_ptr<LocalTableStorage> LocalStorage::GetStorage(const DataTable &table) {
	return table_manager.GetStorage(table);
}

void LocalStorage::VerifyNewConstraint(Context &context, DataTable &parent, const BoundConstraint &constraint) {
	auto storage = table_manager.GetStorage(parent);
	if (!storage) {
		return;
	}
	auto flush_key = storage->LockShared();
	storage->GetCollection().VerifyNewConstraint(context, transaction, parent, constraint);
}

} // namespace duckdb
