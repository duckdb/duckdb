//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/duck_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {
class DuckTransaction;

//! The Transaction Manager is responsible for creating and managing
//! transactions
class DuckTransactionManager : public TransactionManager {
	friend struct CheckpointLock;

public:
	explicit DuckTransactionManager(AttachedDatabase &db);
	~DuckTransactionManager();

public:
	static DuckTransactionManager &Get(AttachedDatabase &db);

	//! Start a new transaction
	Transaction *StartTransaction(ClientContext &context) override;
	//! Commit the given transaction
	string CommitTransaction(ClientContext &context, Transaction *transaction) override;
	//! Rollback the given transaction
	void RollbackTransaction(Transaction *transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	transaction_t LowestActiveId() {
		return lowest_active_id;
	}
	transaction_t LowestActiveStart() {
		return lowest_active_start;
	}

	bool IsDuckTransactionManager() override {
		return true;
	}

private:
	bool CanCheckpoint(optional_ptr<DuckTransaction> current = nullptr);
	//! Remove the given transaction from the list of active transactions
	void RemoveTransaction(DuckTransaction &transaction) noexcept;
	void LockClients(vector<ClientLockWrapper> &client_locks, ClientContext &context);

private:
	//! The current start timestamp used by transactions
	transaction_t current_start_timestamp;
	//! The current transaction ID used by transactions
	transaction_t current_transaction_id;
	//! The lowest active transaction id
	atomic<transaction_t> lowest_active_id;
	//! The lowest active transaction timestamp
	atomic<transaction_t> lowest_active_start;
	//! Set of currently running transactions
	vector<unique_ptr<DuckTransaction>> active_transactions;
	//! Set of recently committed transactions
	vector<unique_ptr<DuckTransaction>> recently_committed_transactions;
	//! Transactions awaiting GC
	vector<unique_ptr<DuckTransaction>> old_transactions;
	//! The lock used for transaction operations
	mutex transaction_lock;

	bool thread_is_checkpointing;
};

} // namespace duckdb
