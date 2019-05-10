//===----------------------------------------------------------------------===//
//                         DuckDB
//
// transaction/transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "transaction/undo_buffer.hpp"

namespace duckdb {

extern transaction_t TRANSACTION_ID_START;
extern transaction_t MAXIMUM_QUERY_ID;

class CatalogEntry;
class DataTable;
class StorageChunk;
class WriteAheadLog;

struct VersionInformation {
	DataTable *table;
	StorageChunk *chunk;
	union {
		uint64_t entry;
		VersionInformation *pointer;
	} prev;
	VersionInformation *next;
	transaction_t version_number;
	uint8_t *tuple_data;
};

//! The transaction object holds information about a currently running or past
//! transaction

class Transaction {
public:
	Transaction(transaction_t start_time, transaction_t transaction_id)
	    : start_time(start_time), transaction_id(transaction_id), commit_id(0), highest_active_query(0),
	      active_query(MAXIMUM_QUERY_ID) {
	}

	void PushCatalogEntry(CatalogEntry *entry);
	//! Create deleted entries in the undo buffer
	void PushDeletedEntries(uint64_t offset, uint64_t count, StorageChunk *storage,
	                        VersionInformation *version_pointers[]);
	//! Push an old tuple version in the undo buffer
	void PushTuple(UndoFlags flag, uint64_t offset, StorageChunk *storage);
	//! Push a query into the undo buffer, this will be written to the WAL for
	//! redo purposes

	void PushQuery(string query);

	//! Commit the current transaction with the given commit identifier
	void Commit(WriteAheadLog *log, transaction_t commit_id);
	//! Rollback
	void Rollback() {
		undo_buffer.Rollback();
	}
	//! Cleanup the undo buffer
	void Cleanup() {
		undo_buffer.Cleanup();
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

private:
	uint8_t *PushTuple(UndoFlags flag, uint64_t data_size);

	//! The undo buffer is used to store old versions of rows that are updated
	//! or deleted
	UndoBuffer undo_buffer;

	Transaction(const Transaction &) = delete;
};

} // namespace duckdb
