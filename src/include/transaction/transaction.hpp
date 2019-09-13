//===----------------------------------------------------------------------===//
//                         DuckDB
//
// transaction/transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "common/types/data_chunk.hpp"
#include "common/unordered_map.hpp"
#include "transaction/undo_buffer.hpp"
#include "transaction/local_storage.hpp"

namespace duckdb {
class SequenceCatalogEntry;

class CatalogEntry;
class DataTable;
class VersionChunk;
class VersionChunkInfo;
class WriteAheadLog;

struct DeleteInfo;
struct VersionInfo;

//! The transaction object holds information about a currently running or past
//! transaction

class Transaction {
public:
	Transaction(transaction_t start_time, transaction_t transaction_id, timestamp_t start_timestamp)
	    : start_time(start_time), transaction_id(transaction_id), commit_id(0), highest_active_query(0),
	      active_query(MAXIMUM_QUERY_ID), start_timestamp(start_timestamp) {
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
public:
	void PushCatalogEntry(CatalogEntry *entry);
	//! Push an old tuple version in the undo buffer
	void PushTuple(UndoFlags flag, index_t offset, VersionChunk *storage);
	//! Push a query into the undo buffer
	void PushQuery(string query);

	//! Checks whether or not the transaction can be successfully committed,
	void CheckCommit();
	//! Commit the current transaction with the given commit identifier
	void Commit(WriteAheadLog *log, transaction_t commit_id) noexcept;
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

	data_ptr_t PushTuple(UndoFlags flag, index_t data_size);

	void PushDelete(VersionChunkInfo *vinfo, row_t row);

private:
	//! The undo buffer is used to store old versions of rows that are updated
	//! or deleted
	UndoBuffer undo_buffer;

	Transaction(const Transaction &) = delete;
};

} // namespace duckdb
