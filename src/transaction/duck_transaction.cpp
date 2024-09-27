#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include "duckdb/transaction/append_info.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

TransactionData::TransactionData(DuckTransaction &transaction_p) // NOLINT
    : transaction(&transaction_p), transaction_id(transaction_p.transaction_id), start_time(transaction_p.start_time) {
}
TransactionData::TransactionData(transaction_t transaction_id_p, transaction_t start_time_p)
    : transaction(nullptr), transaction_id(transaction_id_p), start_time(start_time_p) {
}

DuckTransaction::DuckTransaction(DuckTransactionManager &manager, ClientContext &context_p, transaction_t start_time,
                                 transaction_t transaction_id, idx_t catalog_version_p)
    : Transaction(manager, context_p), start_time(start_time), transaction_id(transaction_id), commit_id(0),
      highest_active_query(0), catalog_version(catalog_version_p), transaction_manager(manager), undo_buffer(context_p),
      storage(make_uniq<LocalStorage>(context_p, *this)) {
}

DuckTransaction::~DuckTransaction() {
}

DuckTransaction &DuckTransaction::Get(ClientContext &context, AttachedDatabase &db) {
	return DuckTransaction::Get(context, db.GetCatalog());
}

DuckTransaction &DuckTransaction::Get(ClientContext &context, Catalog &catalog) {
	auto &transaction = Transaction::Get(context, catalog);
	if (!transaction.IsDuckTransaction()) {
		throw InternalException("DuckTransaction::Get called on non-DuckDB transaction");
	}
	return transaction.Cast<DuckTransaction>();
}

LocalStorage &DuckTransaction::GetLocalStorage() {
	return *storage;
}

void DuckTransaction::PushCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data, idx_t extra_data_size) {
	idx_t alloc_size = sizeof(CatalogEntry *);
	if (extra_data_size > 0) {
		alloc_size += extra_data_size + sizeof(idx_t);
	}

	auto baseptr = undo_buffer.CreateEntry(UndoFlags::CATALOG_ENTRY, alloc_size);
	// store the pointer to the catalog entry
	Store<CatalogEntry *>(&entry, baseptr);
	if (extra_data_size > 0) {
		// copy the extra data behind the catalog entry pointer (if any)
		baseptr += sizeof(CatalogEntry *);
		// first store the extra data size
		Store<idx_t>(extra_data_size, baseptr);
		baseptr += sizeof(idx_t);
		// then copy over the actual data
		memcpy(baseptr, extra_data, extra_data_size);
	}
}

void DuckTransaction::PushDelete(DataTable &table, RowVersionManager &info, idx_t vector_idx, row_t rows[], idx_t count,
                                 idx_t base_row) {
	bool is_consecutive = true;
	// check if the rows are consecutive
	for (idx_t i = 0; i < count; i++) {
		if (rows[i] != row_t(i)) {
			is_consecutive = false;
			break;
		}
	}
	idx_t alloc_size = sizeof(DeleteInfo);
	if (!is_consecutive) {
		// if rows are not consecutive we need to allocate row identifiers
		alloc_size += sizeof(uint16_t) * count;
	}

	auto delete_info = reinterpret_cast<DeleteInfo *>(undo_buffer.CreateEntry(UndoFlags::DELETE_TUPLE, alloc_size));
	delete_info->version_info = &info;
	delete_info->vector_idx = vector_idx;
	delete_info->table = &table;
	delete_info->count = count;
	delete_info->base_row = base_row;
	delete_info->is_consecutive = is_consecutive;
	if (!is_consecutive) {
		// if rows are not consecutive
		auto delete_rows = delete_info->GetRows();
		for (idx_t i = 0; i < count; i++) {
			delete_rows[i] = NumericCast<uint16_t>(rows[i]);
		}
	}
}

void DuckTransaction::PushAppend(DataTable &table, idx_t start_row, idx_t row_count) {
	auto append_info =
	    reinterpret_cast<AppendInfo *>(undo_buffer.CreateEntry(UndoFlags::INSERT_TUPLE, sizeof(AppendInfo)));
	append_info->table = &table;
	append_info->start_row = start_row;
	append_info->count = row_count;
}

UpdateInfo *DuckTransaction::CreateUpdateInfo(idx_t type_size, idx_t entries) {
	data_ptr_t base_info = undo_buffer.CreateEntry(
	    UndoFlags::UPDATE_TUPLE, sizeof(UpdateInfo) + (sizeof(sel_t) + type_size) * STANDARD_VECTOR_SIZE);
	auto update_info = reinterpret_cast<UpdateInfo *>(base_info);
	update_info->max = STANDARD_VECTOR_SIZE;
	update_info->tuples = reinterpret_cast<sel_t *>(base_info + sizeof(UpdateInfo));
	update_info->tuple_data = base_info + sizeof(UpdateInfo) + sizeof(sel_t) * update_info->max;
	update_info->version_number = transaction_id;
	return update_info;
}

void DuckTransaction::PushSequenceUsage(SequenceCatalogEntry &sequence, const SequenceData &data) {
	lock_guard<mutex> l(sequence_lock);
	auto entry = sequence_usage.find(sequence);
	if (entry == sequence_usage.end()) {
		auto sequence_ptr = undo_buffer.CreateEntry(UndoFlags::SEQUENCE_VALUE, sizeof(SequenceValue));
		auto sequence_info = reinterpret_cast<SequenceValue *>(sequence_ptr);
		sequence_info->entry = &sequence;
		sequence_info->usage_count = data.usage_count;
		sequence_info->counter = data.counter;
		sequence_usage.emplace(sequence, *sequence_info);
	} else {
		auto &sequence_info = entry->second.get();
		D_ASSERT(RefersToSameObject(*sequence_info.entry, sequence));
		sequence_info.usage_count = data.usage_count;
		sequence_info.counter = data.counter;
	}
}

void DuckTransaction::UpdateCollection(shared_ptr<RowGroupCollection> &collection) {
	auto collection_ref = reference<RowGroupCollection>(*collection);
	auto entry = updated_collections.find(collection_ref);
	if (entry != updated_collections.end()) {
		// already exists
		return;
	}
	updated_collections.insert(make_pair(collection_ref, collection));
}

bool DuckTransaction::ChangesMade() {
	return undo_buffer.ChangesMade() || storage->ChangesMade();
}

UndoBufferProperties DuckTransaction::GetUndoProperties() {
	return undo_buffer.GetProperties();
}

bool DuckTransaction::AutomaticCheckpoint(AttachedDatabase &db, const UndoBufferProperties &properties) {
	if (!ChangesMade()) {
		// read-only transactions cannot trigger an automated checkpoint
		return false;
	}
	if (db.IsReadOnly()) {
		// when attaching a database in read-only mode we cannot checkpoint
		// note that attaching a database in read-only mode does NOT mean we never make changes
		// WAL replay can make changes to the database - but only in the in-memory copy of the
		return false;
	}
	auto &storage_manager = db.GetStorageManager();
	return storage_manager.AutomaticCheckpoint(storage->EstimatedSize() + properties.estimated_size);
}

bool DuckTransaction::ShouldWriteToWAL(AttachedDatabase &db) {
	if (!ChangesMade()) {
		return false;
	}
	if (db.IsSystem()) {
		return false;
	}
	auto &storage_manager = db.GetStorageManager();
	auto log = storage_manager.GetWAL();
	if (!log) {
		return false;
	}
	return true;
}

ErrorData DuckTransaction::WriteToWAL(AttachedDatabase &db, unique_ptr<StorageCommitState> &commit_state) noexcept {
	try {
		D_ASSERT(ShouldWriteToWAL(db));
		auto &storage_manager = db.GetStorageManager();
		auto log = storage_manager.GetWAL();
		commit_state = storage_manager.GenStorageCommitState(*log);
		storage->Commit(commit_state.get());
		undo_buffer.WriteToWAL(*log, commit_state.get());
		if (commit_state->HasRowGroupData()) {
			// if we have optimistically written any data AND we are writing to the WAL, we have written references to
			// optimistically written blocks
			// hence we need to ensure those optimistically written blocks are persisted
			storage_manager.GetBlockManager().FileSync();
		}
	} catch (std::exception &ex) {
		if (commit_state) {
			commit_state->RevertCommit();
			commit_state.reset();
		}
		return ErrorData(ex);
	}
	return ErrorData();
}

ErrorData DuckTransaction::Commit(AttachedDatabase &db, transaction_t new_commit_id,
                                  unique_ptr<StorageCommitState> commit_state) noexcept {
	// "checkpoint" parameter indicates if the caller will checkpoint. If checkpoint ==
	//    true: Then this function will NOT write to the WAL or flush/persist.
	//          This method only makes commit in memory, expecting caller to checkpoint/flush.
	//    false: Then this function WILL write to the WAL and Flush/Persist it.
	this->commit_id = new_commit_id;
	if (!ChangesMade()) {
		// no need to flush anything if we made no changes
		return ErrorData();
	}
	D_ASSERT(db.IsSystem() || db.IsTemporary() || !IsReadOnly());

	UndoBuffer::IteratorState iterator_state;
	try {
		storage->Commit(commit_state.get());
		undo_buffer.Commit(iterator_state, commit_id);
		if (commit_state) {
			// if we have written to the WAL - flush after the commit has been successful
			commit_state->FlushCommit();
		}
		return ErrorData();
	} catch (std::exception &ex) {
		undo_buffer.RevertCommit(iterator_state, this->transaction_id);
		if (commit_state) {
			// if we have written to the WAL - truncate the WAL on failure
			commit_state->RevertCommit();
		}
		return ErrorData(ex);
	}
}

void DuckTransaction::Rollback() noexcept {
	storage->Rollback();
	undo_buffer.Rollback();
}

void DuckTransaction::Cleanup(transaction_t lowest_active_transaction) {
	undo_buffer.Cleanup(lowest_active_transaction);
}

void DuckTransaction::SetReadWrite() {
	Transaction::SetReadWrite();
	// obtain a shared checkpoint lock to prevent concurrent checkpoints while this transaction is running
	write_lock = transaction_manager.SharedCheckpointLock();
}

unique_ptr<StorageLockKey> DuckTransaction::TryGetCheckpointLock() {
	if (!write_lock) {
		throw InternalException("TryUpgradeCheckpointLock - but thread has no shared lock!?");
	}
	return transaction_manager.TryUpgradeCheckpointLock(*write_lock);
}

shared_ptr<CheckpointLock> DuckTransaction::SharedLockTable(DataTableInfo &info) {
	lock_guard<mutex> l(active_locks_lock);
	auto entry = active_locks.find(info);
	if (entry != active_locks.end()) {
		// found an existing lock
		auto lock_weak_ptr = entry->second;
		// check if it is expired
		auto lock = lock_weak_ptr.lock();
		if (lock) {
			// not expired - return it
			return lock;
		}
	}
	// no existing lock - obtain it
	auto table_lock = info.GetSharedLock();
	auto checkpoint_lock = make_shared_ptr<CheckpointLock>(std::move(table_lock));
	// insert it into the active locks and return it
	active_locks.insert(make_pair(std::ref(info), checkpoint_lock));
	return checkpoint_lock;
}

} // namespace duckdb
