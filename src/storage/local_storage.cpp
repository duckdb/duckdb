#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/partial_block_manager.hpp"

#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Local Table Storage
//===--------------------------------------------------------------------===//
LocalTableStorage::LocalTableStorage(DataTable &table)
    : table(&table), allocator(Allocator::Get(table.db)), deleted_rows(0) {
	auto types = table.GetTypes();
	row_groups = make_shared<RowGroupCollection>(table.info, TableIOManager::Get(table).GetBlockManagerForRowData(),
	                                             types, MAX_ROW_ID, 0);

	stats.InitializeEmpty(types);
	table.info->indexes.Scan([&](Index &index) {
		D_ASSERT(index.type == IndexType::ART);
		auto &art = (ART &)index;
		if (art.constraint_type != IndexConstraintType::NONE) {
			// unique index: create a local ART index that maintains the same unique constraint
			vector<unique_ptr<Expression>> unbound_expressions;
			for (auto &expr : art.unbound_expressions) {
				unbound_expressions.push_back(expr->Copy());
			}
			indexes.AddIndex(make_unique<ART>(art.column_ids, art.table_io_manager, move(unbound_expressions),
			                                  art.constraint_type, art.db));
		}
		return false;
	});
}

LocalTableStorage::LocalTableStorage(DataTable &new_dt, LocalTableStorage &parent, idx_t changed_idx,
                                     const LogicalType &target_type, const vector<column_t> &bound_columns,
                                     Expression &cast_expr)
    : table(&new_dt), allocator(Allocator::Get(table->db)), deleted_rows(parent.deleted_rows),
      partial_manager(move(parent.partial_manager)), written_blocks(move(parent.written_blocks)) {
	if (partial_manager) {
		partial_manager->FlushPartialBlocks();
	}
	stats.InitializeAlterType(parent.stats, changed_idx, target_type);
	row_groups =
	    parent.row_groups->AlterType(changed_idx, target_type, bound_columns, cast_expr, stats.GetStats(changed_idx));
	parent.row_groups.reset();
	indexes.Move(parent.indexes);
}

LocalTableStorage::LocalTableStorage(DataTable &new_dt, LocalTableStorage &parent, idx_t drop_idx)
    : table(&new_dt), allocator(Allocator::Get(table->db)), deleted_rows(parent.deleted_rows),
      partial_manager(move(parent.partial_manager)), written_blocks(move(parent.written_blocks)) {
	if (partial_manager) {
		partial_manager->FlushPartialBlocks();
	}
	stats.InitializeRemoveColumn(parent.stats, drop_idx);
	row_groups = parent.row_groups->RemoveColumn(drop_idx);
	parent.row_groups.reset();
	indexes.Move(parent.indexes);
}

LocalTableStorage::LocalTableStorage(DataTable &new_dt, LocalTableStorage &parent, ColumnDefinition &new_column,
                                     Expression *default_value)
    : table(&new_dt), allocator(Allocator::Get(table->db)), deleted_rows(parent.deleted_rows),
      partial_manager(move(parent.partial_manager)), written_blocks(move(parent.written_blocks)) {
	idx_t new_column_idx = parent.table->column_definitions.size();
	stats.InitializeAddColumn(parent.stats, new_column.GetType());
	row_groups = parent.row_groups->AddColumn(new_column, default_value, stats.GetStats(new_column_idx));
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

//===--------------------------------------------------------------------===//
// LocalStorage
//===--------------------------------------------------------------------===//
LocalStorage::LocalStorage(Transaction &transaction) : transaction(transaction) {
}

LocalStorage &LocalStorage::Get(Transaction &transaction) {
	return transaction.GetLocalStorage();
}

LocalStorage &LocalStorage::Get(ClientContext &context) {
	return Transaction::GetTransaction(context).GetLocalStorage();
}

void LocalStorage::InitializeScan(DataTable *table, CollectionScanState &state, TableFilterSet *table_filters) {
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		return;
	}
	auto storage = entry->second.get();
	storage->InitializeScan(state, table_filters);
}

void LocalStorage::Scan(CollectionScanState &state, const vector<column_t> &column_ids, DataChunk &result) {
	state.Scan(transaction, result);
}

void LocalStorage::InitializeParallelScan(DataTable *table, ParallelCollectionScanState &state) {
	auto storage = GetStorage(table);
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
	auto storage = GetStorage(table);
	if (!storage) {
		return false;
	}
	return storage->row_groups->NextParallelScan(context, state, scan_state);
}

void LocalStorage::InitializeAppend(LocalAppendState &state, DataTable *table) {
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		auto new_storage = make_shared<LocalTableStorage>(*table);
		state.storage = new_storage.get();
		table_storage.insert(make_pair(table, move(new_storage)));
	} else {
		state.storage = entry->second.get();
	}
	state.storage->row_groups->InitializeAppend(state.append_state);
}

void LocalStorage::Append(LocalAppendState &state, DataChunk &chunk) {
	// append to unique indices (if any)
	auto storage = state.storage;
	idx_t base_id = MAX_ROW_ID + storage->row_groups->GetTotalRows();
	if (!DataTable::AppendToIndexes(storage->indexes, chunk, base_id)) {
		throw ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicated key");
	}

	//! Append the chunk to the local storage
	auto new_row_group = storage->row_groups->Append(chunk, state.append_state, storage->stats);

	//! Check if we should pre-emptively flush blocks to disk
	if (new_row_group) {
		storage->CheckFlushToDisk();
	}
}

void LocalTableStorage::CheckFlushToDisk() {
	// we finished writing a complete row group
	// check if we should pre-emptively write it to disk
	if (table->info->IsTemporary() || StorageManager::GetStorageManager(table->db).InMemory()) {
		return;
	}
	if (deleted_rows != 0) {
		// we have deletes - we cannot merge
		return;
	}
	// we should! write the second-to-last row group to disk
	// allocate the partial block-manager if none is allocated yet
	if (!partial_manager) {
		auto &block_manager = table->info->table_io_manager->GetBlockManagerForRowData();
		partial_manager = make_unique<PartialBlockManager>(block_manager);
	}
	// flush second-to-last row group
	auto row_group = row_groups->GetRowGroup(-2);
	FlushToDisk(row_group);
}

void LocalTableStorage::FlushToDisk(RowGroup *row_group) {
	// flush the specified row group
	D_ASSERT(row_group);
	D_ASSERT(deleted_rows == 0);
	D_ASSERT(partial_manager);
	//! The set of column compression types (if any)
	vector<CompressionType> compression_types;
	D_ASSERT(compression_types.empty());
	for (auto &column : table->column_definitions) {
		compression_types.push_back(column.CompressionType());
	}
	auto row_group_pointer = row_group->WriteToDisk(*partial_manager, compression_types);
	for (idx_t col_idx = 0; col_idx < row_group_pointer.statistics.size(); col_idx++) {
		row_group_pointer.states[col_idx]->GetBlockIds(written_blocks);
		stats.MergeStats(col_idx, *row_group_pointer.statistics[col_idx]);
	}
}
void LocalTableStorage::FlushToDisk() {
	// no partial manager - nothing to flush
	if (!partial_manager) {
		return;
	}
	// flush the last row group
	FlushToDisk(row_groups->GetRowGroup(-1));
	// then flush the partial manager
	partial_manager->FlushPartialBlocks();
	partial_manager.reset();
}

void LocalStorage::FinalizeAppend(LocalAppendState &state) {
	TransactionData transaction_data(0, 0);
	state.storage->row_groups->FinalizeAppend(transaction_data, state.append_state);
}

LocalTableStorage *LocalStorage::GetStorage(DataTable *table) {
	auto entry = table_storage.find(table);
	return entry == table_storage.end() ? nullptr : entry->second.get();
}

idx_t LocalStorage::EstimatedSize() {
	idx_t estimated_size = 0;
	for (auto &storage : table_storage) {
		estimated_size += storage.second->EstimatedSize();
	}
	return estimated_size;
}

bool LocalTableStorage::HasWrittenBlocks() {
	return partial_manager || !written_blocks.empty();
}

idx_t LocalStorage::Delete(DataTable *table, Vector &row_ids, idx_t count) {
	auto storage = GetStorage(table);
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

void LocalStorage::Update(DataTable *table, Vector &row_ids, const vector<column_t> &column_ids, DataChunk &updates) {
	auto storage = GetStorage(table);
	D_ASSERT(storage);

	auto ids = FlatVector::GetData<row_t>(row_ids);
	storage->row_groups->Update(TransactionData(0, 0), ids, column_ids, updates, storage->stats);
}

template <class T>
bool LocalTableStorage::ScanTableStorage(Transaction &transaction, const vector<column_t> &column_ids, T &&fun) {
	auto all_types = table->GetTypes();
	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		scan_types.push_back(all_types[column_ids[i]]);
	}
	DataChunk chunk;
	chunk.Initialize(allocator, scan_types);

	// initialize the scan
	TableScanState state;
	state.Initialize(column_ids, nullptr);
	InitializeScan(state.local_state, nullptr);

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

template <class T>
bool LocalTableStorage::ScanTableStorage(Transaction &transaction, T &&fun) {
	vector<column_t> column_ids;
	column_ids.reserve(table->column_definitions.size());
	for (idx_t i = 0; i < table->column_definitions.size(); i++) {
		column_ids.push_back(i);
	}
	return ScanTableStorage(transaction, column_ids, fun);
}

void LocalTableStorage::AppendToIndexes(Transaction &transaction, TableAppendState &append_state, idx_t append_count,
                                        bool append_to_table) {
	bool constraint_violated = false;
	if (append_to_table) {
		table->InitializeAppend(transaction, append_state, append_count);
	}
	if (append_to_table) {
		// appending: need to scan entire
		ScanTableStorage(transaction, [&](DataChunk &chunk) -> bool {
			// append this chunk to the indexes of the table
			if (!table->AppendToIndexes(chunk, append_state.current_row)) {
				constraint_violated = true;
				return false;
			}
			// append to base table
			table->Append(chunk, append_state);
			return true;
		});
	} else {
		// only need to scan for index append
		// figure out which columns we need to scan for the set of indexes
		auto columns = table->info->indexes.GetRequiredColumns();
		// create an empty mock chunk that contains all the correct types for the table
		DataChunk mock_chunk;
		mock_chunk.InitializeEmpty(table->GetTypes());
		ScanTableStorage(transaction, columns, [&](DataChunk &chunk) -> bool {
			// construct the mock chunk by referencing the required columns
			for (idx_t i = 0; i < columns.size(); i++) {
				mock_chunk.data[columns[i]].Reference(chunk.data[i]);
			}
			mock_chunk.SetCardinality(chunk);
			// append this chunk to the indexes of the table
			if (!table->AppendToIndexes(mock_chunk, append_state.current_row)) {
				constraint_violated = true;
				return false;
			}
			append_state.current_row += chunk.size();
			return true;
		});
	}
	if (constraint_violated) {
		// need to revert the append
		row_t current_row = append_state.row_start;
		// remove the data from the indexes, if there are any indexes
		ScanTableStorage(transaction, [&](DataChunk &chunk) -> bool {
			// append this chunk to the indexes of the table
			table->RemoveFromIndexes(append_state, chunk, current_row);

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
		throw ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicated key");
	}
}

void LocalStorage::Flush(DataTable &table, LocalTableStorage &storage) {
	// bulk append threshold: a full row group
	static constexpr const idx_t MERGE_THRESHOLD = RowGroup::ROW_GROUP_SIZE;

	auto storage_entry = move(table_storage[&table]);
	table_storage[&table].reset();

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
		table.MergeStorage(*storage.row_groups, storage.indexes, storage.stats);
	} else {
		if (storage.partial_manager || !storage.written_blocks.empty()) {
			// we have written data but cannot merge to disk after all
			// revert the data we have already written
			storage.Rollback();
		}
		// append to the indexes and append to the base table
		storage.AppendToIndexes(transaction, append_state, append_count, true);
	}
	transaction.PushAppend(&table, append_state.row_start, append_count);
}

void LocalStorage::Commit(LocalStorage::CommitState &commit_state, Transaction &transaction) {
	// commit local storage, iterate over all entries in the table storage map
	for (auto &entry : table_storage) {
		auto table = entry.first;
		auto storage = entry.second.get();
		Flush(*table, *storage);
	}
	// finished commit: clear local storage
	table_storage.clear();
}

void LocalStorage::Rollback() {
	for (auto &entry : table_storage) {
		auto storage = entry.second.get();
		if (!storage) {
			continue;
		}
		storage->Rollback();
	}
}

void LocalTableStorage::Rollback() {
	if (partial_manager) {
		partial_manager->Clear();
		partial_manager.reset();
	}
	auto &block_manager = table->info->table_io_manager->GetBlockManagerForRowData();
	for (auto block_id : written_blocks) {
		block_manager.MarkBlockAsModified(block_id);
	}
}

idx_t LocalStorage::AddedRows(DataTable *table) {
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		return 0;
	}
	return entry->second->row_groups->GetTotalRows() - entry->second->deleted_rows;
}

void LocalStorage::MoveStorage(DataTable *old_dt, DataTable *new_dt) {
	// check if there are any pending appends for the old version of the table
	auto entry = table_storage.find(old_dt);
	if (entry == table_storage.end()) {
		return;
	}
	// take over the storage from the old entry
	auto new_storage = move(entry->second);
	new_storage->table = new_dt;
	table_storage.erase(entry);
	table_storage[new_dt] = move(new_storage);
}

void LocalStorage::AddColumn(DataTable *old_dt, DataTable *new_dt, ColumnDefinition &new_column,
                             Expression *default_value) {
	// check if there are any pending appends for the old version of the table
	auto entry = table_storage.find(old_dt);
	if (entry == table_storage.end()) {
		return;
	}
	auto storage = move(entry->second);
	auto new_storage = make_unique<LocalTableStorage>(*new_dt, *storage, new_column, default_value);

	table_storage[new_dt] = move(new_storage);
	table_storage.erase(old_dt);
}

void LocalStorage::DropColumn(DataTable *old_dt, DataTable *new_dt, idx_t removed_column) {
	// check if there are any pending appends for the old version of the table
	auto entry = table_storage.find(old_dt);
	if (entry == table_storage.end()) {
		return;
	}
	auto storage = move(entry->second);
	auto new_storage = make_unique<LocalTableStorage>(*new_dt, *storage, removed_column);

	table_storage[new_dt] = move(new_storage);
	table_storage.erase(old_dt);
}

void LocalStorage::ChangeType(DataTable *old_dt, DataTable *new_dt, idx_t changed_idx, const LogicalType &target_type,
                              const vector<column_t> &bound_columns, Expression &cast_expr) {
	// check if there are any pending appends for the old version of the table
	auto entry = table_storage.find(old_dt);
	if (entry == table_storage.end()) {
		return;
	}
	auto storage = move(entry->second);
	auto new_storage =
	    make_unique<LocalTableStorage>(*new_dt, *storage, changed_idx, target_type, bound_columns, cast_expr);

	table_storage[new_dt] = move(new_storage);
	table_storage.erase(old_dt);
}

void LocalStorage::FetchChunk(DataTable *table, Vector &row_ids, idx_t count, DataChunk &verify_chunk) {
	auto storage = GetStorage(table);

	ColumnFetchState fetch_state;
	vector<column_t> col_ids;
	vector<LogicalType> types = storage->table->GetTypes();
	for (idx_t i = 0; i < types.size(); i++) {
		col_ids.push_back(i);
	}
	verify_chunk.Initialize(storage->allocator, types);
	storage->row_groups->Fetch(transaction, verify_chunk, col_ids, row_ids, count, fetch_state);
}

TableIndexList &LocalStorage::GetIndexes(DataTable *table) {
	auto storage = GetStorage(table);

	return storage->indexes;
}

void LocalStorage::VerifyNewConstraint(DataTable &parent, const BoundConstraint &constraint) {
	auto storage = GetStorage(&parent);
	if (!storage) {
		return;
	}
	storage->row_groups->VerifyNewConstraint(parent, constraint);
}

} // namespace duckdb
