#include "duckdb/transaction/local_storage.hpp"

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &table)
    : context(context), table_ref(table), allocator(Allocator::Get(table.db)), deleted_rows(0),
      optimistic_writer(context, table) {
	auto types = table.GetTypes();
	auto data_table_info = table.GetDataTableInfo();
	row_groups = optimistic_writer.CreateCollection(table, types, OptimisticWritePartialManagers::GLOBAL);
	auto &collection = *row_groups->collection;
	collection.InitializeEmpty();

	for (auto &index : data_table_info->GetIndexes().Indexes()) {
		auto constraint = index.GetConstraintType();
		if (constraint == IndexConstraintType::NONE) {
			continue;
		}
		if (!index.IsBound()) {
			continue;
		}
		auto &bound_index = index.Cast<BoundIndex>();
		if (!bound_index.SupportsDeltaIndexes()) {
			continue;
		}

		// Create a delete index and a local index.
		auto delete_index = bound_index.CreateDeltaIndex(DeltaIndexType::LOCAL_DELETE);
		delete_indexes.AddIndex(std::move(delete_index));

		auto append_index = bound_index.CreateDeltaIndex(DeltaIndexType::LOCAL_APPEND);
		append_indexes.AddIndex(std::move(append_index));
	}
}

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &new_data_table, LocalTableStorage &parent,
                                     const idx_t alter_column_index, const LogicalType &target_type,
                                     const vector<StorageIndex> &bound_columns, Expression &cast_expr)
    : context(context), table_ref(new_data_table), allocator(Allocator::Get(new_data_table.db)),
      deleted_rows(parent.deleted_rows), optimistic_collections(std::move(parent.optimistic_collections)),
      optimistic_writer(new_data_table, parent.optimistic_writer) {
	// Alter the column type.
	auto &parent_collection = *parent.row_groups->collection;
	auto new_collection =
	    parent_collection.AlterType(context, alter_column_index, target_type, bound_columns, cast_expr);
	parent_collection.CommitDropColumn(alter_column_index);
	row_groups = std::move(parent.row_groups);
	row_groups->collection = std::move(new_collection);

	append_indexes.Move(parent.append_indexes);
}

LocalTableStorage::LocalTableStorage(DataTable &new_data_table, LocalTableStorage &parent,
                                     const idx_t drop_column_index)
    : table_ref(new_data_table), allocator(Allocator::Get(new_data_table.db)), deleted_rows(parent.deleted_rows),
      optimistic_collections(std::move(parent.optimistic_collections)),
      optimistic_writer(new_data_table, parent.optimistic_writer) {
	// Remove the column from the previous table storage.
	auto &parent_collection = *parent.row_groups->collection;
	auto new_collection = parent_collection.RemoveColumn(drop_column_index);
	parent_collection.CommitDropColumn(drop_column_index);
	row_groups = std::move(parent.row_groups);
	row_groups->collection = std::move(new_collection);

	append_indexes.Move(parent.append_indexes);
}

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &new_dt, LocalTableStorage &parent,
                                     ColumnDefinition &new_column, ExpressionExecutor &default_executor)
    : table_ref(new_dt), allocator(Allocator::Get(new_dt.db)), deleted_rows(parent.deleted_rows),
      optimistic_collections(std::move(parent.optimistic_collections)),
      optimistic_writer(new_dt, parent.optimistic_writer) {
	auto &parent_collection = *parent.row_groups->collection;
	auto new_collection = parent_collection.AddColumn(context, new_column, default_executor);
	row_groups = std::move(parent.row_groups);
	row_groups->collection = std::move(new_collection);
	append_indexes.Move(parent.append_indexes);
}

LocalTableStorage::~LocalTableStorage() {
}

void LocalTableStorage::InitializeScan(CollectionScanState &state, optional_ptr<TableFilterSet> table_filters) {
	auto &collection = *row_groups->collection;
	if (collection.GetTotalRows() == 0) {
		throw InternalException("No rows in LocalTableStorage row group for scan");
	}
	collection.InitializeScan(context, state, state.GetColumnIds(), table_filters.get());
}

idx_t LocalTableStorage::EstimatedSize() {
	// count the appended rows
	auto &collection = *row_groups->collection;
	idx_t data_size = 0;

	if (collection.GetTotalRows() >= collection.GetRowGroupSize() && deleted_rows == 0) {
		// Optimistic insertion does not generate many WAL logs, so we estimate the size of the Data Block Pointers here
		idx_t row_group_count = row_groups->complete_row_groups + 1;
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

	// get the index size
	idx_t index_sizes = 0;
	for (auto &index : append_indexes.Indexes()) {
		if (!index.IsBound()) {
			continue;
		}
		index_sizes += index.Cast<BoundIndex>().GetInMemorySize();
	}

	// return the size of the appended rows and the index size
	return data_size + index_sizes;
}

void LocalTableStorage::WriteNewRowGroup() {
	if (deleted_rows != 0) {
		// we have deletes - we cannot merge row groups
		return;
	}
	optimistic_writer.WriteNewRowGroup(*row_groups);
}

void LocalTableStorage::FlushBlocks() {
	auto &collection = *row_groups->collection;
	const idx_t row_group_size = collection.GetRowGroupSize();
	if (collection.GetTotalRows() >= row_group_size) {
		// write any unflushed row groups
		optimistic_writer.WriteUnflushedRowGroups(*row_groups);
	}
	optimistic_writer.FinalFlush();
}

ErrorData LocalTableStorage::AppendToIndexes(DuckTransaction &transaction, RowGroupCollection &source,
                                             TableIndexList &index_list, const vector<LogicalType> &table_types,
                                             row_t &start_row) {
	// mapped_column_ids contains the physical column indices of each Indexed column in the table.
	// This mapping is used to retrieve the physical column index for the corresponding vector of an index chunk scan.
	// For example, if we are processing data for index_chunk.data[i], we can retrieve the physical column index
	// by getting the value at mapped_column_ids[i].
	// An important note is that the index_chunk orderings are created in accordance with this mapping, not the other
	// way around. (Check the scan code below, where the mapped_column_ids is passed as a parameter to the scan.
	// The index_chunk inside of that lambda is ordered according to the mapping that is a parameter to the scan).

	// mapped_column_ids is used in two places:
	// 1) To create the physical table chunk in this function.
	// 2) If we are in an unbound state (i.e., WAL replay is happening right now), this mapping and the index_chunk
	//	  are buffered in unbound_index. However, there can also be buffered deletes happening, so it is important
	//    to maintain a canonical representation of the mapping, which is just sorting.
	D_ASSERT(!index_list.Empty());
	auto indexed_columns = index_list.GetRequiredColumns();
	vector<StorageIndex> mapped_column_ids;
	for (auto &col : indexed_columns) {
		mapped_column_ids.emplace_back(col);
	}
	std::sort(mapped_column_ids.begin(), mapped_column_ids.end());
	auto active_checkpoint = transaction.GetTransactionManager().Cast<DuckTransactionManager>().GetActiveCheckpoint();
	auto checkpoint_id = active_checkpoint == MAX_TRANSACTION_ID ? optional_idx() : active_checkpoint;

	// However, because the bound expressions of the indexes (and their bound
	// column references) are in relation to ALL table columns, we create an
	// empty table chunk based on the table types. It references the indexed columns,
	// and contains nothing for all non-indexed columns.
	DataChunk table_chunk;
	table_chunk.InitializeEmpty(table_types);

	// index_chunk scans are created here in the mapped_column_ids ordering (see note above).
	ErrorData error;
	for (auto &index_chunk : source.Chunks(transaction, mapped_column_ids)) {
		D_ASSERT(index_chunk.ColumnCount() == mapped_column_ids.size());
		for (idx_t i = 0; i < mapped_column_ids.size(); i++) {
			auto col_id = mapped_column_ids[i].GetPrimaryIndex();
			table_chunk.data[col_id].Reference(index_chunk.data[i]);
		}
		table_chunk.SetCardinality(index_chunk);

		// Pass both the table and the index chunk.
		// We need the table chunk for the bound indexes,
		// and the index chunk for the unbound indexes (to buffer it).
		error = DataTable::AppendToIndexes(index_list, delete_indexes, table_chunk, index_chunk, mapped_column_ids,
		                                   start_row, index_append_mode, checkpoint_id);
		if (error.HasError()) {
			break;
		}
		start_row += UnsafeNumericCast<row_t>(index_chunk.size());
	}
	return error;
}

void LocalTableStorage::AppendToTable(DuckTransaction &transaction, TableAppendState &append_state) {
	auto &table = table_ref.get();
	table.InitializeAppend(transaction, append_state);
	auto &collection = *row_groups->collection;
	for (auto &table_chunk : collection.Chunks(transaction)) {
		// Append to the base table.
		table.Append(table_chunk, append_state);
	}
	table.FinalizeAppend(transaction, append_state);
}

void LocalTableStorage::AppendToIndexes(DuckTransaction &transaction, TableAppendState &append_state) {
	// In this function, we might scan all table columns,
	// as we might also append to the table itself (append_to_table).
	auto &table = table_ref.get();
	if (!table.HasIndexes()) {
		// no indexes to append to
		return;
	}
	auto data_table_info = table.GetDataTableInfo();
	auto &index_list = data_table_info->GetIndexes();
	auto &collection = *row_groups->collection;
	auto error = AppendToIndexes(transaction, collection, index_list, table.GetTypes(), append_state.current_row);
	if (error.HasError()) {
		// Revert all appended row IDs.
		row_t current_row = append_state.row_start;
		// Remove the data from the indexes, if any.
		for (auto &chunk : collection.Chunks(transaction)) {
			if (current_row >= append_state.current_row) {
				// Finished deleting all rows from the index.
				break;
			}
			// Remove the chunk.
			try {
				table.RevertIndexAppend(append_state, chunk, current_row);
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
		error.Throw();
	}
}

PhysicalIndex LocalTableStorage::CreateOptimisticCollection(unique_ptr<OptimisticWriteCollection> collection) {
	lock_guard<mutex> l(collections_lock);
	optimistic_collections.push_back(std::move(collection));
	return PhysicalIndex(optimistic_collections.size() - 1);
}

OptimisticWriteCollection &LocalTableStorage::GetOptimisticCollection(const PhysicalIndex collection_index) {
	lock_guard<mutex> l(collections_lock);
	auto &collection = optimistic_collections[collection_index.index];
	return *collection;
}

void LocalTableStorage::ResetOptimisticCollection(const PhysicalIndex collection_index) {
	lock_guard<mutex> l(collections_lock);
	optimistic_collections[collection_index.index].reset();
}

OptimisticDataWriter &LocalTableStorage::GetOptimisticWriter() {
	return optimistic_writer;
}

void LocalTableStorage::Rollback() {
	optimistic_writer.Rollback();

	for (auto &collection : optimistic_collections) {
		if (!collection) {
			continue;
		}
		collection->collection->CommitDropTable();
	}
	optimistic_collections.clear();
	row_groups->collection->CommitDropTable();
}

//===--------------------------------------------------------------------===//
// LocalTableManager
//===--------------------------------------------------------------------===//
optional_ptr<LocalTableStorage> LocalTableManager::GetStorage(DataTable &table) const {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	return entry == table_storage.end() ? nullptr : entry->second.get();
}

LocalTableStorage &LocalTableManager::GetOrCreateStorage(ClientContext &context, DataTable &table) {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		auto new_storage = make_shared_ptr<LocalTableStorage>(context, table);
		auto storage = new_storage.get();
		table_storage.insert(make_pair(reference<DataTable>(table), std::move(new_storage)));
		return *storage;
	} else {
		return *entry->second.get();
	}
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

reference_map_t<DataTable, shared_ptr<LocalTableStorage>> LocalTableManager::MoveEntries() {
	lock_guard<mutex> l(table_storage_lock);
	return std::move(table_storage);
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
LocalStorage::LocalStorage(ClientContext &context, DuckTransaction &transaction)
    : context(context), transaction(transaction) {
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

void LocalStorage::InitializeScan(DataTable &table, CollectionScanState &state,
                                  optional_ptr<TableFilterSet> table_filters) {
	auto storage = table_manager.GetStorage(table);
	if (storage == nullptr || storage->GetCollection().GetTotalRows() == 0) {
		return;
	}
	storage->InitializeScan(state, table_filters);
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
		storage->GetCollection().InitializeParallelScan(state);
	}
}

RowGroupCollection &LocalTableStorage::GetCollection() {
	return *row_groups->collection;
}

OptimisticWriteCollection &LocalTableStorage::GetPrimaryCollection() {
	return *row_groups;
}

bool LocalStorage::NextParallelScan(ClientContext &context, DataTable &table, ParallelCollectionScanState &state,
                                    CollectionScanState &scan_state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return false;
	}
	return storage->GetCollection().NextParallelScan(context, state, scan_state);
}

void LocalStorage::InitializeAppend(LocalAppendState &state, DataTable &table) {
	state.storage = &table_manager.GetOrCreateStorage(context, table);
	state.storage->GetCollection().InitializeAppend(TransactionData(transaction), state.append_state);
}

void LocalStorage::InitializeStorage(LocalAppendState &state, DataTable &table) {
	state.storage = &table_manager.GetOrCreateStorage(context, table);
}

void LocalTableStorage::AppendToDeleteIndexes(Vector &row_ids, DataChunk &delete_chunk) {
	if (delete_chunk.size() == 0) {
		return;
	}

	for (auto &index : delete_indexes.Indexes()) {
		D_ASSERT(index.IsBound());
		if (!index.IsUnique()) {
			continue;
		}
		IndexAppendInfo index_append_info(IndexAppendMode::IGNORE_DUPLICATES, nullptr);
		auto result = index.Cast<BoundIndex>().Append(delete_chunk, row_ids, index_append_info);
		if (result.HasError()) {
			throw InternalException("unexpected constraint violation on delete ART: ", result.Message());
		}
	}
}

void LocalStorage::Append(LocalAppendState &state, DataChunk &table_chunk, DataTableInfo &data_table_info) {
	// Append to any unique indexes.
	auto storage = state.storage;
	auto offset = NumericCast<idx_t>(MAX_ROW_ID) + storage->GetCollection().GetTotalRows();
	idx_t base_id = offset + state.append_state.total_append_count;

	if (!storage->append_indexes.Empty()) {
		DataChunk index_chunk;
		vector<StorageIndex> mapped_column_ids;

		// Only initialize the index_chunk, if there are unbound indexes.
		if (storage->append_indexes.HasUnbound() || storage->delete_indexes.HasUnbound()) {
			TableIndexList::InitializeIndexChunk(index_chunk, table_chunk.GetTypes(), mapped_column_ids,
			                                     data_table_info);
			TableIndexList::ReferenceIndexChunk(table_chunk, index_chunk, mapped_column_ids);
		}

		auto error = DataTable::AppendToIndexes(storage->append_indexes, storage->delete_indexes, table_chunk,
		                                        index_chunk, mapped_column_ids, NumericCast<row_t>(base_id),
		                                        storage->index_append_mode, optional_idx());
		if (error.HasError()) {
			error.Throw();
		}
	}

	// Append the chunk to the local storage.
	auto new_row_group = storage->GetCollection().Append(table_chunk, state.append_state);

	// Check if we should pre-emptively flush blocks to disk.
	if (new_row_group) {
		storage->WriteNewRowGroup();
	}
}

void LocalStorage::FinalizeAppend(LocalAppendState &state) {
	state.storage->GetCollection().FinalizeAppend(state.append_state.transaction, state.append_state);
}

void LocalStorage::LocalMerge(DataTable &table, OptimisticWriteCollection &collection) {
	auto &storage = table_manager.GetOrCreateStorage(context, table);
	if (!storage.append_indexes.Empty()) {
		// append data to indexes if required
		row_t base_id = MAX_ROW_ID + NumericCast<row_t>(storage.GetCollection().GetTotalRows());
		auto error = storage.AppendToIndexes(transaction, *collection.collection, storage.append_indexes,
		                                     table.GetTypes(), base_id);
		if (error.HasError()) {
			error.Throw();
		}
	}
	auto &target = storage.GetPrimaryCollection();
	target.MergeStorage(collection);
}

PhysicalIndex LocalStorage::CreateOptimisticCollection(DataTable &table,
                                                       unique_ptr<OptimisticWriteCollection> collection) {
	auto &storage = table_manager.GetOrCreateStorage(context, table);
	return storage.CreateOptimisticCollection(std::move(collection));
}

OptimisticWriteCollection &LocalStorage::GetOptimisticCollection(DataTable &table,
                                                                 const PhysicalIndex collection_index) {
	auto &storage = table_manager.GetOrCreateStorage(context, table);
	return storage.GetOptimisticCollection(collection_index);
}

void LocalStorage::ResetOptimisticCollection(DataTable &table, const PhysicalIndex collection_index) {
	auto &storage = table_manager.GetOrCreateStorage(context, table);
	storage.ResetOptimisticCollection(collection_index);
}

OptimisticDataWriter &LocalStorage::GetOptimisticWriter(DataTable &table) {
	auto &storage = table_manager.GetOrCreateStorage(context, table);
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

idx_t LocalStorage::Delete(DataTable &table, Vector &row_ids, idx_t count) {
	auto storage = table_manager.GetStorage(table);
	D_ASSERT(storage);

	// delete from unique indices (if any)
	if (!storage->append_indexes.Empty()) {
		storage->GetCollection().RemoveFromIndexes(context, storage->append_indexes, row_ids, count,
		                                           IndexRemovalType::MAIN_INDEX_ONLY);
	}

	auto ids = FlatVector::GetData<row_t>(row_ids);
	idx_t delete_count = storage->GetCollection().Delete(TransactionData(0, 0), table, ids, count);
	storage->deleted_rows += delete_count;
	return delete_count;
}

void LocalStorage::Update(DataTable &table, Vector &row_ids, const vector<PhysicalIndex> &column_ids,
                          DataChunk &updates) {
	D_ASSERT(updates.size() >= 1);
	auto storage = table_manager.GetStorage(table);
	D_ASSERT(storage);

	auto ids = FlatVector::GetData<row_t>(row_ids);
	storage->GetCollection().Update(TransactionData(0, 0), table, ids, column_ids, updates);
}

void LocalStorage::Flush(DataTable &table, LocalTableStorage &storage, optional_ptr<StorageCommitState> commit_state) {
	if (storage.is_dropped) {
		return;
	}
	if (storage.GetCollection().GetTotalRows() <= storage.deleted_rows) {
		// all rows that we added were deleted
		// rollback any partial blocks that are still outstanding
		storage.Rollback();
		return;
	}

	auto append_count = storage.GetCollection().GetTotalRows() - storage.deleted_rows;
	const auto row_group_size = storage.GetCollection().GetRowGroupSize();

	TableAppendState append_state;
	table.AppendLock(transaction, append_state);
	if ((append_state.row_start == 0 || storage.GetCollection().GetTotalRows() >= row_group_size) &&
	    storage.deleted_rows == 0) {
		// table is currently empty OR we are bulk appending: move over the storage directly
		// first flush any outstanding blocks
		storage.FlushBlocks();
		// Append to the indexes.
		storage.AppendToIndexes(transaction, append_state);
		// finally move over the row groups
		table.MergeStorage(storage.GetCollection(), commit_state);
	} else {
		// check if we have written data
		// if we have, we cannot merge to disk after all
		// so we need to revert the data we have already written
		storage.Rollback();
		// append to the indexes
		storage.AppendToIndexes(transaction, append_state);
		// after that is successful - append to the table
		storage.AppendToTable(transaction, append_state);
	}
	transaction.PushAppend(table, NumericCast<idx_t>(append_state.row_start), append_count);

#ifdef DEBUG
	// Verify that our index memory is stable.
	table.VerifyIndexBuffers();
#endif
}

void LocalStorage::Commit(optional_ptr<StorageCommitState> commit_state) {
	// commit local storage
	// iterate over all entries in the table storage map and commit them
	// after this, the local storage is no longer required and can be cleared
	auto table_storage = table_manager.MoveEntries();
	for (auto &entry : table_storage) {
		auto table = entry.first;
		auto storage = entry.second.get();
		Flush(table, *storage, commit_state);
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
	return storage->GetCollection().GetTotalRows() - storage->deleted_rows;
}

vector<PartitionStatistics> LocalStorage::GetPartitionStats(DataTable &table) const {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return vector<PartitionStatistics>();
	}
	return storage->GetCollection().GetPartitionStats();
}

void LocalStorage::DropTable(DataTable &table) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return;
	}
	storage->is_dropped = true;
}

void LocalStorage::MoveStorage(DataTable &old_dt, DataTable &new_dt) {
	// check if there are any pending appends for the old version of the table
	auto new_storage = table_manager.MoveEntry(old_dt);
	if (!new_storage) {
		return;
	}
	// take over the storage from the old entry
	new_storage->table_ref = new_dt;
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::AddColumn(DataTable &old_dt, DataTable &new_dt, ColumnDefinition &new_column,
                             ExpressionExecutor &default_executor) {
	// check if there are any pending appends for the old version of the table
	auto storage = table_manager.MoveEntry(old_dt);
	if (!storage) {
		return;
	}
	auto new_storage = make_shared_ptr<LocalTableStorage>(context, new_dt, *storage, new_column, default_executor);
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::DropColumn(DataTable &old_dt, DataTable &new_dt, const idx_t drop_column_index) {
	// check if there are any pending appends for the old version of the table
	auto storage = table_manager.MoveEntry(old_dt);
	if (!storage) {
		return;
	}
	auto new_storage = make_shared_ptr<LocalTableStorage>(new_dt, *storage, drop_column_index);
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::ChangeType(DataTable &old_dt, DataTable &new_dt, idx_t changed_idx, const LogicalType &target_type,
                              const vector<StorageIndex> &bound_columns, Expression &cast_expr) {
	// check if there are any pending appends for the old version of the table
	auto storage = table_manager.MoveEntry(old_dt);
	if (!storage) {
		return;
	}
	auto new_storage = make_shared_ptr<LocalTableStorage>(context, new_dt, *storage, changed_idx, target_type,
	                                                      bound_columns, cast_expr);
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::FetchChunk(DataTable &table, Vector &row_ids, idx_t count, const vector<StorageIndex> &col_ids,
                              DataChunk &chunk, ColumnFetchState &fetch_state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		throw InternalException("LocalStorage::FetchChunk - local storage not found");
	}
	storage->GetCollection().Fetch(transaction, chunk, col_ids, row_ids, count, fetch_state);
}

bool LocalStorage::CanFetch(DataTable &table, const row_t row_id) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		throw InternalException("LocalStorage::CanFetch - local storage not found");
	}
	return storage->GetCollection().CanFetch(transaction, row_id);
}

TableIndexList &LocalStorage::GetIndexes(ClientContext &context, DataTable &table) {
	auto &storage = table_manager.GetOrCreateStorage(context, table);
	return storage.append_indexes;
}

optional_ptr<LocalTableStorage> LocalStorage::GetStorage(DataTable &table) {
	return table_manager.GetStorage(table);
}

void LocalStorage::VerifyNewConstraint(DataTable &parent, const BoundConstraint &constraint) {
	auto storage = table_manager.GetStorage(parent);
	if (!storage) {
		return;
	}
	storage->GetCollection().VerifyNewConstraint(context, parent, constraint);
}

} // namespace duckdb
