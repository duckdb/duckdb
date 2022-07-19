//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/transaction/undo_buffer.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {
class SequenceCatalogEntry;

class ColumnData;
class ClientContext;
class CatalogEntry;
class DataTable;
class DatabaseInstance;
class WriteAheadLog;

class ChunkVectorInfo;

struct DeleteInfo;
struct UpdateInfo;

//! The transaction object holds information about a currently running or past
//! transaction

class Transaction {
public:
	Transaction(weak_ptr<ClientContext> context, transaction_t start_time, transaction_t transaction_id,
	            timestamp_t start_timestamp, idx_t catalog_version);

	weak_ptr<ClientContext> context;
	//! The start timestamp of this transaction
	transaction_t start_time;
	//! The transaction id of this transaction
	transaction_t transaction_id;
	//! The commit id of this transaction, if it has successfully been committed
	transaction_t commit_id;
	//! Highest active query when the transaction finished, used for cleaning up
	transaction_t highest_active_query;
	//! The current active query for the transaction. Set to MAXIMUM_QUERY_ID if
	//! no query is active.
	atomic<transaction_t> active_query;
	//! The timestamp when the transaction started
	timestamp_t start_timestamp;
	//! The catalog version when the transaction was started
	idx_t catalog_version;
	//! The set of uncommitted appends for the transaction
	LocalStorage storage;
	//! Map of all sequences that were used during the transaction and the value they had in this transaction
	unordered_map<SequenceCatalogEntry *, SequenceValue> sequence_usage;
	//! Whether or not the transaction has been invalidated
	bool is_invalidated;

public:
	static Transaction &GetTransaction(ClientContext &context);

	void PushCatalogEntry(CatalogEntry *entry, data_ptr_t extra_data = nullptr, idx_t extra_data_size = 0);

	//! Commit the current transaction with the given commit identifier. Returns an error message if the transaction
	//! commit failed, or an empty string if the commit was sucessful
	string Commit(DatabaseInstance &db, transaction_t commit_id, bool checkpoint) noexcept;
	//! Returns whether or not a commit of this transaction should trigger an automatic checkpoint
	bool AutomaticCheckpoint(DatabaseInstance &db);

	//! Rollback
	void Rollback() noexcept {
		undo_buffer.Rollback();
	}
	//! Cleanup the undo buffer
	void Cleanup() {
		undo_buffer.Cleanup();
	}

	void Invalidate() {
		is_invalidated = true;
	}
	bool IsInvalidated() {
		return is_invalidated;
	}
	bool ChangesMade();

	timestamp_t GetCurrentTransactionStartTimestamp() {
		return start_timestamp;
	}

	void PushDelete(DataTable *table, ChunkVectorInfo *vinfo, row_t rows[], idx_t count, idx_t base_row);
	void PushAppend(DataTable *table, idx_t row_start, idx_t row_count);
	UpdateInfo *CreateUpdateInfo(idx_t type_size, idx_t entries);

private:
	//! The undo buffer is used to store old versions of rows that are updated
	//! or deleted
	UndoBuffer undo_buffer;

	Transaction(const Transaction &) = delete;
};

} // namespace duckdb
