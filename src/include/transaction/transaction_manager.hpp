//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// transaction/transaction_manager.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <mutex>

#include "transaction/transaction.hpp"

namespace duckdb {

extern transaction_t TRANSACTION_ID_START;

//! The Transaction Manager is responsible for creating and managing
//! transactions
class TransactionManager {
  public:
	TransactionManager();
	//! Start a new transaction
	Transaction *StartTransaction();
	//! Commit the given transaction
	void CommitTransaction(Transaction *transaction);
	//! Rollback the given transaction
	void RollbackTransaction(Transaction *transaction);

  private:
	//! Remove the given transaction from the list of active transactions
	void RemoveTransaction(Transaction *transaction);

	//! The current start timestamp used by transactions
	transaction_t current_start_timestamp;
	//! The current transaction ID used by transactions
	transaction_t current_transaction_id;
	//! Set of currently running transactions
	std::vector<std::unique_ptr<Transaction>> active_transactions;
	//! Set of recently committed transactions, whose UndoBuffers are necessary
	//! for active transactions
	std::vector<std::unique_ptr<Transaction>> recently_committed_transactions;
	//! The lock used for transaction operations
	std::mutex transaction_lock;
};

} // namespace duckdb