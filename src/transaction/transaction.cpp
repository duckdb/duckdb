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
#include "duckdb/main/config.hpp"
#include "duckdb/storage/table/column_data.hpp"

#include <cstring>

namespace duckdb {

Transaction::Transaction(weak_ptr<ClientContext> context_p, transaction_t start_time, transaction_t transaction_id,
                         timestamp_t start_timestamp, idx_t catalog_version)
    : context(move(context_p)), start_time(start_time), transaction_id(transaction_id), commit_id(0),
      highest_active_query(0), active_query(MAXIMUM_QUERY_ID), start_timestamp(start_timestamp),
      catalog_version(catalog_version), storage(*this), is_invalidated(false), undo_buffer(context.lock()) {
}

Transaction &Transaction::GetTransaction(ClientContext &context) {
	return context.ActiveTransaction();
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
	return undo_buffer.ChangesMade() || storage.ChangesMade();
}

bool Transaction::AutomaticCheckpoint(DatabaseInstance &db) {
	auto &config = DBConfig::GetConfig(db);
	auto &storage_manager = StorageManager::GetStorageManager(db);
	auto log = storage_manager.GetWriteAheadLog();
	if (!log) {
		return false;
	}

	auto initial_size = log->GetWALSize();
	idx_t expected_wal_size = initial_size + storage.EstimatedSize() + undo_buffer.EstimatedSize();
	return expected_wal_size > config.options.checkpoint_wal_size;
}

string Transaction::Commit(DatabaseInstance &db, transaction_t commit_id, bool checkpoint) noexcept {
	this->commit_id = commit_id;
	auto &storage_manager = StorageManager::GetStorageManager(db);
	auto log = storage_manager.GetWriteAheadLog();

	UndoBuffer::IteratorState iterator_state;
	LocalStorage::CommitState commit_state;
	idx_t initial_wal_size = 0;
	idx_t initial_written = 0;
	if (log) {
		auto initial_size = log->GetWALSize();
		initial_written = log->GetTotalWritten();
		initial_wal_size = initial_size < 0 ? 0 : idx_t(initial_size);
	} else {
		D_ASSERT(!checkpoint);
	}
	try {
		if (checkpoint) {
			// check if we are checkpointing after this commit
			// if we are checkpointing, we don't need to write anything to the WAL
			// this saves us a lot of unnecessary writes to disk in the case of large commits
			log->skip_writing = true;
		}
		storage.Commit(commit_state, *this, log, commit_id);
		undo_buffer.Commit(iterator_state, log, commit_id);
		if (log) {
			// commit any sequences that were used to the WAL
			for (auto &entry : sequence_usage) {
				log->WriteSequenceValue(entry.first, entry.second);
			}
			// flush the WAL if any changes were made
			if (log->GetTotalWritten() > initial_written) {
				D_ASSERT(!checkpoint);
				D_ASSERT(!log->skip_writing);
				log->Flush();
			}
			log->skip_writing = false;
		}
		return string();
	} catch (std::exception &ex) {
		undo_buffer.RevertCommit(iterator_state, transaction_id);
		if (log) {
			log->skip_writing = false;
			if (log->GetTotalWritten() > initial_written) {
				// remove any entries written into the WAL by truncating it
				log->Truncate(initial_wal_size);
			}
		}
		D_ASSERT(!log || !log->skip_writing);
		return ex.what();
	}
}

} // namespace duckdb
