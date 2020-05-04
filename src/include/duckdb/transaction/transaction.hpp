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

namespace duckdb {
class SequenceCatalogEntry;

class ClientContext;
class CatalogEntry;
class DataTable;
class WriteAheadLog;

class ChunkInfo;

struct DeleteInfo;
struct UpdateInfo;

//! The transaction object holds information about a currently running or past
//! transaction

class Transaction {
public:
	Transaction(transaction_t start_time, transaction_t transaction_id, timestamp_t start_timestamp)
	    : start_time(start_time), transaction_id(transaction_id), commit_id(0), highest_active_query(0),
	      active_query(MAXIMUM_QUERY_ID), start_timestamp(start_timestamp), is_invalidated(false) {
	}

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
	transaction_t active_query;
	//! The timestamp when the transaction started
	timestamp_t start_timestamp;
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
	string Commit(WriteAheadLog *log, transaction_t commit_id) noexcept;
	//! Rollback
	void Rollback() noexcept {
		undo_buffer.Rollback();
	}
	//! Cleanup the undo buffer
	void Cleanup() {
		undo_buffer.Cleanup();
	}

	timestamp_t GetCurrentTransactionStartTimestamp() {
		return start_timestamp;
	}

	void PushDelete(DataTable *table, ChunkInfo *vinfo, row_t rows[], idx_t count, idx_t base_row);

	UpdateInfo *CreateUpdateInfo(idx_t type_size, idx_t entries);

private:
	//! The undo buffer is used to store old versions of rows that are updated
	//! or deleted
	UndoBuffer undo_buffer;

	Transaction(const Transaction &) = delete;
};

} // namespace duckdb
