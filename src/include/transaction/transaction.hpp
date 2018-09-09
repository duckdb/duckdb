//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// transaction/transaction.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"

#include "transaction/undo_buffer.hpp"

namespace duckdb {

class AbstractCatalogEntry;
class DataTable;
class StorageChunk;

struct VersionInformation {
	StorageChunk *chunk;
	union {
		size_t entry;
		VersionInformation *pointer;
	} prev;
	std::shared_ptr<VersionInformation> next;
	transaction_t version_number;
	void *tuple_data;
};

//! The transaction object holds information about a currently running or past
//! transaction
class Transaction {
  public:
	Transaction(transaction_t start_time, transaction_t transaction_id)
	    : start_time(start_time), transaction_id(transaction_id), commit_id(0) {
	}

	void PushCatalogEntry(AbstractCatalogEntry *entry);
	//! Create deleted entries in the 
	void PushDeletedEntries(size_t offset, size_t count, StorageChunk *storage, std::shared_ptr<VersionInformation> version_pointers[]);

	void Commit(transaction_t commit_id);

	void Rollback() {
		undo_buffer.Rollback();
	}

	//! The start timestamp of this transaction
	transaction_t start_time;
	//! The transaction id of this transaction
	transaction_t transaction_id;
	//! The commit id of this transaction, if it has successfully been committed
	transaction_t commit_id;

  private:
  	void* PushTuple(size_t data_size);

	//! The undo buffer is used to store old versions of rows that are updated
	//! or deleted
	UndoBuffer undo_buffer;

	Transaction(const Transaction &) = delete;
};

} // namespace duckdb
