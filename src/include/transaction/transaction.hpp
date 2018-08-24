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

#include "transaction/undo_buffer.hpp"

namespace duckdb {

class AbstractCatalogEntry;

//! The transaction object holds information about a currently running or past
//! transaction
class Transaction {
  public:
	Transaction(transaction_t start_time, transaction_t transaction_id)
	    : start_time(start_time), transaction_id(transaction_id), commit_id(0) {
	}

	void PushCatalogEntry(AbstractCatalogEntry *entry);

	void Commit(transaction_t commit_id) {
		this->commit_id = commit_id;
		undo_buffer.Commit(commit_id);
	}

	void Rollback() { undo_buffer.Rollback(); }

	//! The start timestamp of this transaction
	transaction_t start_time;
	//! The transaction id of this transaction
	transaction_t transaction_id;
	//! The commit id of this transaction, if it has successfully been committed
	transaction_t commit_id;

  private:
	//! The undo buffer is used to store old versions of rows that are updated
	//! or deleted
	UndoBuffer undo_buffer;

	Transaction(const Transaction &) = delete;
};

} // namespace duckdb
