#include "duckdb/transaction/transaction.hpp"

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

#include <cstring>

namespace duckdb {

TransactionData::TransactionData(Transaction &transaction_p) // NOLINT
    : transaction(&transaction_p), transaction_id(transaction_p.transaction_id), start_time(transaction_p.start_time) {
}
TransactionData::TransactionData(transaction_t transaction_id_p, transaction_t start_time_p)
    : transaction(nullptr), transaction_id(transaction_id_p), start_time(start_time_p) {
}

Transaction::Transaction(ClientContext &context_p, transaction_t start_time, transaction_t transaction_id,
                         timestamp_t start_timestamp, idx_t catalog_version)
    : context(context_p.shared_from_this()), start_time(start_time), transaction_id(transaction_id), commit_id(0),
      highest_active_query(0), active_query(MAXIMUM_QUERY_ID), start_timestamp(start_timestamp),
      catalog_version(catalog_version), temporary_objects(context_p.client_data->temporary_objects),
      undo_buffer(context.lock()), storage(make_unique<LocalStorage>(context_p, *this)) {
}

Transaction::~Transaction() {
}

Transaction &Transaction::GetTransaction(ClientContext &context) {
	return context.ActiveTransaction();
}

LocalStorage &Transaction::GetLocalStorage() {
	return *storage;
}

void Transaction::PushCatalogEntry(CatalogEntry *entry, data_ptr_t extra_data, idx_t extra_data_size) {
	idx_t alloc_size = sizeof(CatalogEntry *);
	if (extra_data_size > 0) {
		alloc_size += extra_data_size + sizeof(idx_t);
	}
	auto baseptr = undo_buffer.CreateEntry(UndoFlags::CATALOG_ENTRY, alloc_size);
	// store the pointer to the catalog entry
	Store<CatalogEntry *>(entry, baseptr);
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

void Transaction::PushDelete(DataTable *table, ChunkVectorInfo *vinfo, row_t rows[], idx_t count, idx_t base_row) {
	auto delete_info =
	    (DeleteInfo *)undo_buffer.CreateEntry(UndoFlags::DELETE_TUPLE, sizeof(DeleteInfo) + sizeof(row_t) * count);
	delete_info->vinfo = vinfo;
	delete_info->table = table;
	delete_info->count = count;
	delete_info->base_row = base_row;
	memcpy(delete_info->rows, rows, sizeof(row_t) * count);
}

void Transaction::PushAppend(DataTable *table, idx_t start_row, idx_t row_count) {
	auto append_info = (AppendInfo *)undo_buffer.CreateEntry(UndoFlags::INSERT_TUPLE, sizeof(AppendInfo));
	append_info->table = table;
	append_info->start_row = start_row;
	append_info->count = row_count;
}

UpdateInfo *Transaction::CreateUpdateInfo(idx_t type_size, idx_t entries) {
	auto update_info = (UpdateInfo *)undo_buffer.CreateEntry(
	    UndoFlags::UPDATE_TUPLE, sizeof(UpdateInfo) + (sizeof(sel_t) + type_size) * STANDARD_VECTOR_SIZE);
	update_info->max = STANDARD_VECTOR_SIZE;
	update_info->tuples = (sel_t *)(((data_ptr_t)update_info) + sizeof(UpdateInfo));
	update_info->tuple_data = ((data_ptr_t)update_info) + sizeof(UpdateInfo) + sizeof(sel_t) * update_info->max;
	update_info->version_number = transaction_id;
	return update_info;
}

bool Transaction::ChangesMade() {
	return undo_buffer.ChangesMade() || storage->ChangesMade();
}

bool Transaction::AutomaticCheckpoint(AttachedDatabase &db) {
	auto &storage_manager = db.GetStorageManager();
	return storage_manager.AutomaticCheckpoint(storage->EstimatedSize() + undo_buffer.EstimatedSize());
}

string Transaction::Commit(AttachedDatabase &db, transaction_t commit_id, bool checkpoint) noexcept {
	// "checkpoint" parameter indicates if the caller will checkpoint. If checkpoint ==
	//    true: Then this function will NOT write to the WAL or flush/persist.
	//          This method only makes commit in memory, expecting caller to checkpoint/flush.
	//    false: Then this function WILL write to the WAL and Flush/Persist it.
	this->commit_id = commit_id;
	auto &storage_manager = db.GetStorageManager();
	auto log = storage_manager.GetWriteAheadLog();

	UndoBuffer::IteratorState iterator_state;
	LocalStorage::CommitState commit_state;
	auto storage_commit_state = storage_manager.GenStorageCommitState(*this, checkpoint);
	try {
		storage->Commit(commit_state, *this);
		undo_buffer.Commit(iterator_state, log, commit_id);
		if (log) {
			// commit any sequences that were used to the WAL
			for (auto &entry : sequence_usage) {
				log->WriteSequenceValue(entry.first, entry.second);
			}
		}
		storage_commit_state->FlushCommit();
		return string();
	} catch (std::exception &ex) {
		undo_buffer.RevertCommit(iterator_state, transaction_id);
		return ex.what();
	}
}

void Transaction::Rollback() noexcept {
	storage->Rollback();
	undo_buffer.Rollback();
}

void Transaction::Cleanup() {
	undo_buffer.Cleanup();
}

ValidChecker &ValidChecker::Get(Transaction &transaction) {
	return transaction.transaction_validity;
}

} // namespace duckdb
