#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/partial_block_manager.hpp"

#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// OptimisticDataWriter
//===--------------------------------------------------------------------===//
OptimisticDataWriter::OptimisticDataWriter(DataTable *table) : table(table) {
}

OptimisticDataWriter::OptimisticDataWriter(DataTable *table, OptimisticDataWriter &parent)
    : table(table), partial_manager(std::move(parent.partial_manager)),
      written_blocks(std::move(parent.written_blocks)) {
	if (partial_manager) {
		partial_manager->FlushPartialBlocks();
	}
}

OptimisticDataWriter::~OptimisticDataWriter() {
}

bool OptimisticDataWriter::PrepareWrite() {
	// check if we should pre-emptively write the table to disk
	if (table->info->IsTemporary() || StorageManager::Get(table->info->db).InMemory()) {
		return false;
	}
	// we should! write the second-to-last row group to disk
	// allocate the partial block-manager if none is allocated yet
	if (!partial_manager) {
		auto &block_manager = table->info->table_io_manager->GetBlockManagerForRowData();
		partial_manager = make_uniq<PartialBlockManager>(block_manager);
	}
	return true;
}

void OptimisticDataWriter::CheckFlushToDisk(RowGroupCollection &row_groups) {
	// we finished writing a complete row group
	if (!PrepareWrite()) {
		return;
	}
	// flush second-to-last row group
	auto row_group = row_groups.GetRowGroup(-2);
	FlushToDisk(row_group);
}

void OptimisticDataWriter::FlushToDisk(RowGroup *row_group) {
	// flush the specified row group
	D_ASSERT(row_group);
	//! The set of column compression types (if any)
	vector<CompressionType> compression_types;
	D_ASSERT(compression_types.empty());
	for (auto &column : table->column_definitions) {
		compression_types.push_back(column.CompressionType());
	}
	auto row_group_pointer = row_group->WriteToDisk(*partial_manager, compression_types);

	// update the set of written blocks
	for (idx_t col_idx = 0; col_idx < row_group_pointer.statistics.size(); col_idx++) {
		row_group_pointer.states[col_idx]->GetBlockIds(written_blocks);
	}
}

void OptimisticDataWriter::FlushToDisk(RowGroupCollection &row_groups, bool force) {
	if (!partial_manager) {
		if (!force) {
			// no partial manager - nothing to flush
			return;
		}
		if (!PrepareWrite()) {
			return;
		}
	}
	// flush the last row group
	FlushToDisk(row_groups.GetRowGroup(-1));
}

void OptimisticDataWriter::FinalFlush() {
	if (!partial_manager) {
		return;
	}
	// then flush the partial manager
	partial_manager->FlushPartialBlocks();
	partial_manager.reset();
}

void OptimisticDataWriter::Rollback() {
	if (partial_manager) {
		partial_manager->Clear();
		partial_manager.reset();
	}
	if (!written_blocks.empty()) {
		auto &block_manager = table->info->table_io_manager->GetBlockManagerForRowData();
		for (auto block_id : written_blocks) {
			block_manager.MarkBlockAsFree(block_id);
		}
	}
}

//===--------------------------------------------------------------------===//
// Local Table Storage
//===--------------------------------------------------------------------===//
LocalTableStorage::LocalTableStorage(DataTable &table)
    : table(&table), allocator(Allocator::Get(table.db)), deleted_rows(0), optimistic_writer(&table) {

	auto types = table.GetTypes();
	row_groups = make_shared<RowGroupCollection>(table.info, TableIOManager::Get(table).GetBlockManagerForRowData(),
	                                             types, MAX_ROW_ID, 0);
	row_groups->InitializeEmpty();

	table.info->indexes.Scan([&](Index &index) {
		D_ASSERT(index.type == IndexType::ART);
		auto &art = (ART &)index;
		if (art.constraint_type != IndexConstraintType::NONE) {
			// unique index: create a local ART index that maintains the same unique constraint
			vector<unique_ptr<Expression>> unbound_expressions;
			for (auto &expr : art.unbound_expressions) {
				unbound_expressions.push_back(expr->Copy());
			}
			indexes.AddIndex(make_uniq<ART>(art.column_ids, art.table_io_manager, std::move(unbound_expressions),
			                                  art.constraint_type, art.db));
		}
		return false;
	});
}

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &new_dt, LocalTableStorage &parent,
                                     idx_t changed_idx, const LogicalType &target_type,
                                     const vector<column_t> &bound_columns, Expression &cast_expr)
    : table(&new_dt), allocator(Allocator::Get(table->db)), deleted_rows(parent.deleted_rows),
      optimistic_writer(table, parent.optimistic_writer), optimistic_writers(std::move(parent.optimistic_writers)) {
	row_groups = parent.row_groups->AlterType(context, changed_idx, target_type, bound_columns, cast_expr);
	parent.row_groups.reset();
	indexes.Move(parent.indexes);
}

LocalTableStorage::LocalTableStorage(DataTable &new_dt, LocalTableStorage &parent, idx_t drop_idx)
    : table(&new_dt), allocator(Allocator::Get(table->db)), deleted_rows(parent.deleted_rows),
      optimistic_writer(table, parent.optimistic_writer), optimistic_writers(std::move(parent.optimistic_writers)) {
	row_groups = parent.row_groups->RemoveColumn(drop_idx);
	parent.row_groups.reset();
	indexes.Move(parent.indexes);
}

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &new_dt, LocalTableStorage &parent,
                                     ColumnDefinition &new_column, Expression *default_value)
    : table(&new_dt), allocator(Allocator::Get(table->db)), deleted_rows(parent.deleted_rows),
      optimistic_writer(table, parent.optimistic_writer), optimistic_writers(std::move(parent.optimistic_writers)) {
	row_groups = parent.row_groups->AddColumn(context, new_column, default_value);
	parent.row_groups.reset();
	indexes.Move(parent.indexes);
}

LocalTableStorage::~LocalTableStorage() {
}

void LocalTableStorage::InitializeScan(CollectionScanState &state, TableFilterSet *table_filters) {
	if (row_groups->GetTotalRows() == 0) {
		// nothing to scan
		return;
	}
	row_groups->InitializeScan(state, state.GetColumnIds(), table_filters);
}

idx_t LocalTableStorage::EstimatedSize() {
	idx_t appended_rows = row_groups->GetTotalRows() - deleted_rows;
	if (appended_rows == 0) {
		return 0;
	}
	idx_t row_size = 0;
	auto &types = row_groups->GetTypes();
	for (auto &type : types) {
		row_size += GetTypeIdSize(type.InternalType());
	}
	return appended_rows * row_size;
}

void LocalTableStorage::CheckFlushToDisk() {
	if (deleted_rows != 0) {
		// we have deletes - we cannot merge row groups
		return;
	}
	optimistic_writer.CheckFlushToDisk(*row_groups);
}

void LocalTableStorage::FlushToDisk() {
	optimistic_writer.FlushToDisk(*row_groups);
	optimistic_writer.FinalFlush();
}

PreservedError LocalTableStorage::AppendToIndexes(DuckTransaction &transaction, RowGroupCollection &source,
                                                  TableIndexList &index_list, const vector<LogicalType> &table_types,
                                                  row_t &start_row) {
	// only need to scan for index append
	// figure out which columns we need to scan for the set of indexes
	auto columns = index_list.GetRequiredColumns();
	// create an empty mock chunk that contains all the correct types for the table
	DataChunk mock_chunk;
	mock_chunk.InitializeEmpty(table_types);
	PreservedError error;
	source.Scan(transaction, columns, [&](DataChunk &chunk) -> bool {
		// construct the mock chunk by referencing the required columns
		for (idx_t i = 0; i < columns.size(); i++) {
			mock_chunk.data[columns[i]].Reference(chunk.data[i]);
		}
		mock_chunk.SetCardinality(chunk);
		// append this chunk to the indexes of the table
		error = DataTable::AppendToIndexes(index_list, mock_chunk, start_row);
		if (error) {
			return false;
		}
		start_row += chunk.size();
		return true;
	});
	return error;
}

void LocalTableStorage::AppendToIndexes(DuckTransaction &transaction, TableAppendState &append_state,
                                        idx_t append_count, bool append_to_table) {
	if (append_to_table) {
		table->InitializeAppend(transaction, append_state, append_count);
	}
	PreservedError error;
	if (append_to_table) {
		// appending: need to scan entire
		row_groups->Scan(transaction, [&](DataChunk &chunk) -> bool {
			// append this chunk to the indexes of the table
			error = table->AppendToIndexes(chunk, append_state.current_row);
			if (error) {
				return false;
			}
			// append to base table
			table->Append(chunk, append_state);
			return true;
		});
	} else {
		error = AppendToIndexes(transaction, *row_groups, table->info->indexes, table->GetTypes(),
		                        append_state.current_row);
	}
	if (error) {
		// need to revert the append
		row_t current_row = append_state.row_start;
		// remove the data from the indexes, if there are any indexes
		row_groups->Scan(transaction, [&](DataChunk &chunk) -> bool {
			// append this chunk to the indexes of the table
			try {
				table->RemoveFromIndexes(append_state, chunk, current_row);
			} catch (Exception &ex) {
				error = PreservedError(ex);
				return false;
			} catch (std::exception &ex) {
				error = PreservedError(ex);
				return false;
			}

			current_row += chunk.size();
			if (current_row >= append_state.current_row) {
				// finished deleting all rows from the index: abort now
				return false;
			}
			return true;
		});
		if (append_to_table) {
			table->RevertAppendInternal(append_state.row_start, append_count);
		}
		error.Throw();
	}
}

OptimisticDataWriter *LocalTableStorage::CreateOptimisticWriter() {
	auto writer = make_uniq<OptimisticDataWriter>(table);
	optimistic_writers.push_back(std::move(writer));
	return optimistic_writers.back().get();
}

void LocalTableStorage::Rollback() {
	optimistic_writer.Rollback();
	for (auto &writer : optimistic_writers) {
		writer->Rollback();
	}
	optimistic_writers.clear();
}

//===--------------------------------------------------------------------===//
// LocalTableManager
//===--------------------------------------------------------------------===//
LocalTableStorage *LocalTableManager::GetStorage(DataTable *table) {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	return entry == table_storage.end() ? nullptr : entry->second.get();
}

LocalTableStorage *LocalTableManager::GetOrCreateStorage(DataTable *table) {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		auto new_storage = make_shared<LocalTableStorage>(*table);
		auto storage = new_storage.get();
		table_storage.insert(make_pair(table, std::move(new_storage)));
		return storage;
	} else {
		return entry->second.get();
	}
}

bool LocalTableManager::IsEmpty() {
	lock_guard<mutex> l(table_storage_lock);
	return table_storage.empty();
}

shared_ptr<LocalTableStorage> LocalTableManager::MoveEntry(DataTable *table) {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		return nullptr;
	}
	auto storage_entry = std::move(entry->second);
	table_storage.erase(table);
	return storage_entry;
}

unordered_map<DataTable *, shared_ptr<LocalTableStorage>> LocalTableManager::MoveEntries() {
	lock_guard<mutex> l(table_storage_lock);
	return std::move(table_storage);
}

idx_t LocalTableManager::EstimatedSize() {
	lock_guard<mutex> l(table_storage_lock);
	idx_t estimated_size = 0;
	for (auto &storage : table_storage) {
		estimated_size += storage.second->EstimatedSize();
	}
	return estimated_size;
}

void LocalTableManager::InsertEntry(DataTable *table, shared_ptr<LocalTableStorage> entry) {
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

void LocalStorage::InitializeScan(DataTable *table, CollectionScanState &state, TableFilterSet *table_filters) {
	auto storage = table_manager.GetStorage(table);
	if (storage == nullptr) {
		return;
	}
	storage->InitializeScan(state, table_filters);
}

void LocalStorage::Scan(CollectionScanState &state, const vector<column_t> &column_ids, DataChunk &result) {
	state.Scan(transaction, result);
}

void LocalStorage::InitializeParallelScan(DataTable *table, ParallelCollectionScanState &state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		state.max_row = 0;
		state.vector_index = 0;
		state.current_row_group = nullptr;
	} else {
		storage->row_groups->InitializeParallelScan(state);
	}
}

bool LocalStorage::NextParallelScan(ClientContext &context, DataTable *table, ParallelCollectionScanState &state,
                                    CollectionScanState &scan_state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return false;
	}
	return storage->row_groups->NextParallelScan(context, state, scan_state);
}

void LocalStorage::InitializeAppend(LocalAppendState &state, DataTable *table) {
	state.storage = table_manager.GetOrCreateStorage(table);
	state.storage->row_groups->InitializeAppend(TransactionData(transaction), state.append_state, 0);
}

void LocalStorage::Append(LocalAppendState &state, DataChunk &chunk) {
	// append to unique indices (if any)
	auto storage = state.storage;
	idx_t base_id = MAX_ROW_ID + storage->row_groups->GetTotalRows() + state.append_state.total_append_count;
	auto error = DataTable::AppendToIndexes(storage->indexes, chunk, base_id);
	if (error) {
		error.Throw();
	}

	//! Append the chunk to the local storage
	auto new_row_group = storage->row_groups->Append(chunk, state.append_state);

	//! Check if we should pre-emptively flush blocks to disk
	if (new_row_group) {
		storage->CheckFlushToDisk();
	}
}

void LocalStorage::FinalizeAppend(LocalAppendState &state) {
	state.storage->row_groups->FinalizeAppend(state.append_state.transaction, state.append_state);
}

void LocalStorage::LocalMerge(DataTable *table, RowGroupCollection &collection) {
	auto storage = table_manager.GetOrCreateStorage(table);
	if (!storage->indexes.Empty()) {
		// append data to indexes if required
		row_t base_id = MAX_ROW_ID + storage->row_groups->GetTotalRows();
		auto error = storage->AppendToIndexes(transaction, collection, storage->indexes, table->GetTypes(), base_id);
		if (error) {
			error.Throw();
		}
	}
	storage->row_groups->MergeStorage(collection);
}

OptimisticDataWriter *LocalStorage::CreateOptimisticWriter(DataTable *table) {
	auto storage = table_manager.GetOrCreateStorage(table);
	return storage->CreateOptimisticWriter();
}

bool LocalStorage::ChangesMade() noexcept {
	return !table_manager.IsEmpty();
}

bool LocalStorage::Find(DataTable *table) {
	return table_manager.GetStorage(table) != nullptr;
}

idx_t LocalStorage::EstimatedSize() {
	return table_manager.EstimatedSize();
}

idx_t LocalStorage::Delete(DataTable *table, Vector &row_ids, idx_t count) {
	auto storage = table_manager.GetStorage(table);
	D_ASSERT(storage);

	// delete from unique indices (if any)
	if (!storage->indexes.Empty()) {
		storage->row_groups->RemoveFromIndexes(storage->indexes, row_ids, count);
	}

	auto ids = FlatVector::GetData<row_t>(row_ids);
	idx_t delete_count = storage->row_groups->Delete(TransactionData(0, 0), table, ids, count);
	storage->deleted_rows += delete_count;
	return delete_count;
}

void LocalStorage::Update(DataTable *table, Vector &row_ids, const vector<PhysicalIndex> &column_ids,
                          DataChunk &updates) {
	auto storage = table_manager.GetStorage(table);
	D_ASSERT(storage);

	auto ids = FlatVector::GetData<row_t>(row_ids);
	storage->row_groups->Update(TransactionData(0, 0), ids, column_ids, updates);
}

void LocalStorage::Flush(DataTable &table, LocalTableStorage &storage) {
	if (storage.row_groups->GetTotalRows() <= storage.deleted_rows) {
		return;
	}
	idx_t append_count = storage.row_groups->GetTotalRows() - storage.deleted_rows;

	TableAppendState append_state;
	table.AppendLock(append_state);
	if ((append_state.row_start == 0 || storage.row_groups->GetTotalRows() >= MERGE_THRESHOLD) &&
	    storage.deleted_rows == 0) {
		// table is currently empty OR we are bulk appending: move over the storage directly
		// first flush any out-standing storage nodes
		storage.FlushToDisk();
		// now append to the indexes (if there are any)
		// FIXME: we should be able to merge the transaction-local index directly into the main table index
		// as long we just rewrite some row-ids
		if (!table.info->indexes.Empty()) {
			storage.AppendToIndexes(transaction, append_state, append_count, false);
		}
		// finally move over the row groups
		table.MergeStorage(*storage.row_groups, storage.indexes);
	} else {
		// check if we have written data
		// if we have, we cannot merge to disk after all
		// so we need to revert the data we have already written
		storage.Rollback();
		// append to the indexes and append to the base table
		storage.AppendToIndexes(transaction, append_state, append_count, true);
	}
	transaction.PushAppend(&table, append_state.row_start, append_count);

	// possibly vacuum any excess index data
	table.info->indexes.Scan([&](Index &index) {
		index.Vacuum();
		return false;
	});
}

void LocalStorage::Commit(LocalStorage::CommitState &commit_state, DuckTransaction &transaction) {
	// commit local storage
	// iterate over all entries in the table storage map and commit them
	// after this, the local storage is no longer required and can be cleared
	auto table_storage = table_manager.MoveEntries();
	for (auto &entry : table_storage) {
		auto table = entry.first;
		auto storage = entry.second.get();
		Flush(*table, *storage);
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

idx_t LocalStorage::AddedRows(DataTable *table) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return 0;
	}
	return storage->row_groups->GetTotalRows() - storage->deleted_rows;
}

void LocalStorage::MoveStorage(DataTable *old_dt, DataTable *new_dt) {
	// check if there are any pending appends for the old version of the table
	auto new_storage = table_manager.MoveEntry(old_dt);
	if (!new_storage) {
		return;
	}
	// take over the storage from the old entry
	new_storage->table = new_dt;
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::AddColumn(DataTable *old_dt, DataTable *new_dt, ColumnDefinition &new_column,
                             Expression *default_value) {
	// check if there are any pending appends for the old version of the table
	auto storage = table_manager.MoveEntry(old_dt);
	if (!storage) {
		return;
	}
	auto new_storage = make_shared<LocalTableStorage>(context, *new_dt, *storage, new_column, default_value);
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::DropColumn(DataTable *old_dt, DataTable *new_dt, idx_t removed_column) {
	// check if there are any pending appends for the old version of the table
	auto storage = table_manager.MoveEntry(old_dt);
	if (!storage) {
		return;
	}
	auto new_storage = make_shared<LocalTableStorage>(*new_dt, *storage, removed_column);
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::ChangeType(DataTable *old_dt, DataTable *new_dt, idx_t changed_idx, const LogicalType &target_type,
                              const vector<column_t> &bound_columns, Expression &cast_expr) {
	// check if there are any pending appends for the old version of the table
	auto storage = table_manager.MoveEntry(old_dt);
	if (!storage) {
		return;
	}
	auto new_storage =
	    make_shared<LocalTableStorage>(context, *new_dt, *storage, changed_idx, target_type, bound_columns, cast_expr);
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::FetchChunk(DataTable *table, Vector &row_ids, idx_t count, const vector<column_t> &col_ids,
                              DataChunk &chunk, ColumnFetchState &fetch_state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		throw InternalException("LocalStorage::FetchChunk - local storage not found");
	}

	storage->row_groups->Fetch(transaction, chunk, col_ids, row_ids, count, fetch_state);
}

TableIndexList &LocalStorage::GetIndexes(DataTable *table) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		throw InternalException("LocalStorage::GetIndexes - local storage not found");
	}
	return storage->indexes;
}

void LocalStorage::VerifyNewConstraint(DataTable &parent, const BoundConstraint &constraint) {
	auto storage = table_manager.GetStorage(&parent);
	if (!storage) {
		return;
	}
	storage->row_groups->VerifyNewConstraint(parent, constraint);
}

} // namespace duckdb
