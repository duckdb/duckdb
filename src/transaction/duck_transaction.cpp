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
      catalog_version(catalog_version_p), awaiting_cleanup(false), transaction_manager(manager),
      undo_buffer(*this, context_p), storage(make_uniq<LocalStorage>(context_p, *this)) {
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

	auto undo_entry = undo_buffer.CreateEntry(UndoFlags::CATALOG_ENTRY, alloc_size);
	auto ptr = undo_entry.Ptr();
	// store the pointer to the catalog entry
	Store<CatalogEntry *>(&entry, ptr);
	if (extra_data_size > 0) {
		// copy the extra data behind the catalog entry pointer (if any)
		ptr += sizeof(CatalogEntry *);
		// first store the extra data size
		Store<idx_t>(extra_data_size, ptr);
		ptr += sizeof(idx_t);
		// then copy over the actual data
		memcpy(ptr, extra_data, extra_data_size);
	}
}

void DuckTransaction::PushAttach(AttachedDatabase &db) {
	auto undo_entry = undo_buffer.CreateEntry(UndoFlags::ATTACHED_DATABASE, sizeof(AttachedDatabase *));
	auto ptr = undo_entry.Ptr();
	// store the pointer to the database
	Store<CatalogEntry *>(&db, ptr);
}

void DuckTransaction::PushDelete(DataTable &table, RowVersionManager &info, idx_t vector_idx, row_t rows[], idx_t count,
                                 idx_t base_row) {
	ModifyTable(table);
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

	auto undo_entry = undo_buffer.CreateEntry(UndoFlags::DELETE_TUPLE, alloc_size);
	auto delete_info = reinterpret_cast<DeleteInfo *>(undo_entry.Ptr());
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
	ModifyTable(table);
	auto undo_entry = undo_buffer.CreateEntry(UndoFlags::INSERT_TUPLE, sizeof(AppendInfo));
	auto append_info = reinterpret_cast<AppendInfo *>(undo_entry.Ptr());
	append_info->table = &table;
	append_info->start_row = start_row;
	append_info->count = row_count;
}

UndoBufferReference DuckTransaction::CreateUpdateInfo(idx_t type_size, DataTable &data_table, idx_t entries,
                                                      idx_t row_group_start) {
	idx_t alloc_size = UpdateInfo::GetAllocSize(type_size);
	auto undo_entry = undo_buffer.CreateEntry(UndoFlags::UPDATE_TUPLE, alloc_size);
	auto &update_info = UpdateInfo::Get(undo_entry);
	UpdateInfo::Initialize(update_info, data_table, transaction_id, row_group_start);
	return undo_entry;
}

void DuckTransaction::PushSequenceUsage(SequenceCatalogEntry &sequence, const SequenceData &data) {
	lock_guard<mutex> l(sequence_lock);
	auto entry = sequence_usage.find(sequence);
	if (entry == sequence_usage.end()) {
		auto undo_entry = undo_buffer.CreateEntry(UndoFlags::SEQUENCE_VALUE, sizeof(SequenceValue));
		auto sequence_info = reinterpret_cast<SequenceValue *>(undo_entry.Ptr());
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

void DuckTransaction::ModifyTable(DataTable &tbl) {
	lock_guard<mutex> guard(modified_tables_lock);
	auto table_ref = reference<DataTable>(tbl);
	auto entry = modified_tables.find(table_ref);
	if (entry != modified_tables.end()) {
		// already exists
		return;
	}
	modified_tables.insert(make_pair(table_ref, tbl.shared_from_this()));
}

bool DuckTransaction::ChangesMade() {
	return undo_buffer.ChangesMade() || storage->ChangesMade();
}

UndoBufferProperties DuckTransaction::GetUndoProperties() {
	auto properties = undo_buffer.GetProperties();
	properties.estimated_size += storage->EstimatedSize();
	return properties;
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
	return storage_manager.AutomaticCheckpoint(properties.estimated_size);
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

ErrorData DuckTransaction::WriteToWAL(ClientContext &context, AttachedDatabase &db,
                                      unique_ptr<StorageCommitState> &commit_state) noexcept {
	ErrorData error_data;
	try {
		D_ASSERT(ShouldWriteToWAL(db));
		auto &storage_manager = db.GetStorageManager();
		auto wal = storage_manager.GetWAL();
		commit_state = storage_manager.GenStorageCommitState(*wal);

		auto &profiler = *context.client_data->profiler;

		profiler.StartTimer(MetricsType::COMMIT_LOCAL_STORAGE_LATENCY);
		storage->Commit(commit_state.get());
		profiler.EndTimer(MetricsType::COMMIT_LOCAL_STORAGE_LATENCY);

		profiler.StartTimer(MetricsType::WRITE_TO_WAL_LATENCY);
		undo_buffer.WriteToWAL(*wal, commit_state.get());
		profiler.EndTimer(MetricsType::WRITE_TO_WAL_LATENCY);
		if (commit_state->HasRowGroupData()) {
			// if we have optimistically written any data AND we are writing to the WAL, we have written references to
			// optimistically written blocks
			// hence we need to ensure those optimistically written blocks are persisted
			storage_manager.GetBlockManager().FileSync();
		}
	} catch (std::exception &ex) {
		// Call RevertCommit() outside this try-catch as it itself may throw
		error_data = ErrorData(ex);
	}

	if (commit_state && error_data.HasError()) {
		try {
			commit_state->RevertCommit();
			commit_state.reset();
		} catch (std::exception &) {
			// Ignore this error. If we fail to RevertCommit(), just return the original exception
		}
	}

	return error_data;
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

ErrorData DuckTransaction::Rollback() {
	try {
		storage->Rollback();
		undo_buffer.Rollback();
		return ErrorData();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
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
	unique_lock<mutex> transaction_lock(active_locks_lock);
	auto entry = active_locks.find(info);
	if (entry == active_locks.end()) {
		entry = active_locks.insert(entry, make_pair(std::ref(info), make_uniq<ActiveTableLock>()));
	}
	auto &active_table_lock = *entry->second;
	transaction_lock.unlock(); // release transaction-level lock before acquiring table-level lock
	lock_guard<mutex> table_lock(active_table_lock.checkpoint_lock_mutex);
	auto checkpoint_lock = active_table_lock.checkpoint_lock.lock();
	// check if it is expired (or has never been acquired yet)
	if (checkpoint_lock) {
		// not expired - return it
		return checkpoint_lock;
	}
	// no existing lock - obtain it
	checkpoint_lock = make_shared_ptr<CheckpointLock>(info.GetSharedLock());
	// store it for future reference
	active_table_lock.checkpoint_lock = checkpoint_lock;
	return checkpoint_lock;
}

} // namespace duckdb
