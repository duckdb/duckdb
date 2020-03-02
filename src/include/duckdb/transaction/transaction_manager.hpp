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

#include <atomic>
#include <memory>
#include <mutex>

namespace duckdb {

class ClientContext;
class StorageManager;
class Transaction;

struct StoredCatalogSet {
	//! Stored catalog set
	unique_ptr<CatalogSet> stored_set;
	//! The highest active query number when the catalog set was stored; used for cleaning up
	transaction_t highest_active_query;
};

//! The Transaction Manager is responsible for creating and managing
//! transactions
class TransactionManager {
public:
	TransactionManager(StorageManager &storage);
	~TransactionManager();

	//! Start a new transaction
	Transaction *StartTransaction();
	//! Commit the given transaction
	string CommitTransaction(Transaction *transaction);
	//! Rollback the given transaction
	void RollbackTransaction(Transaction *transaction);
	//! Add the catalog set
	void AddCatalogSet(ClientContext &context, unique_ptr<CatalogSet> catalog_set);

	transaction_t GetQueryNumber() {
		return current_query_number++;
	}

private:
	//! Remove the given transaction from the list of active transactions
	void RemoveTransaction(Transaction *transaction) noexcept;

	//! The current query number
	std::atomic<transaction_t> current_query_number;
	//! The current start timestamp used by transactions
	transaction_t current_start_timestamp;
	//! The current transaction ID used by transactions
	transaction_t current_transaction_id;
	//! Set of currently running transactions
	vector<unique_ptr<Transaction>> active_transactions;
	//! Set of recently committed transactions
	vector<unique_ptr<Transaction>> recently_committed_transactions;
	//! Transactions awaiting GC
	vector<unique_ptr<Transaction>> old_transactions;
	//! Catalog sets
	vector<StoredCatalogSet> old_catalog_sets;
	//! The lock used for transaction operations
	std::mutex transaction_lock;
	//! The storage manager
	StorageManager &storage;
};

} // namespace duckdb
