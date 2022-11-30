//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"

#include "duckdb/common/atomic.hpp"

namespace duckdb {

class AttachedDatabase;
class ClientContext;
class Catalog;
struct ClientLockWrapper;
class DatabaseInstance;
class Transaction;

//! The Transaction Manager is responsible for creating and managing
//! transactions
class TransactionManager {
	friend struct CheckpointLock;

public:
	explicit TransactionManager(AttachedDatabase &db);
	~TransactionManager();

	//! Start a new transaction
	Transaction *StartTransaction(ClientContext &context);
	//! Commit the given transaction
	string CommitTransaction(ClientContext &context, Transaction *transaction);
	//! Rollback the given transaction
	void RollbackTransaction(Transaction *transaction);

	transaction_t GetQueryNumber() {
		return current_query_number++;
	}
	transaction_t LowestActiveId() {
		return lowest_active_id;
	}
	transaction_t LowestActiveStart() {
		return lowest_active_start;
	}

	void Checkpoint(ClientContext &context, bool force = false);

	static TransactionManager &Get(AttachedDatabase &db);

	void SetBaseCommitId(transaction_t base) {
		D_ASSERT(base >= TRANSACTION_ID_START);
		current_transaction_id = base;
	}

private:
	bool CanCheckpoint(Transaction *current = nullptr);
	//! Remove the given transaction from the list of active transactions
	void RemoveTransaction(Transaction *transaction) noexcept;
	void LockClients(vector<ClientLockWrapper> &client_locks, ClientContext &context);

	//! The attached database
	AttachedDatabase &db;
	//! The current query number
	atomic<transaction_t> current_query_number;
	//! The current start timestamp used by transactions
	transaction_t current_start_timestamp;
	//! The current transaction ID used by transactions
	transaction_t current_transaction_id;
	//! The lowest active transaction id
	atomic<transaction_t> lowest_active_id;
	//! The lowest active transaction timestamp
	atomic<transaction_t> lowest_active_start;
	//! Set of currently running transactions
	vector<unique_ptr<Transaction>> active_transactions;
	//! Set of recently committed transactions
	vector<unique_ptr<Transaction>> recently_committed_transactions;
	//! Transactions awaiting GC
	vector<unique_ptr<Transaction>> old_transactions;
	//! The lock used for transaction operations
	mutex transaction_lock;

	bool thread_is_checkpointing;
};

} // namespace duckdb
