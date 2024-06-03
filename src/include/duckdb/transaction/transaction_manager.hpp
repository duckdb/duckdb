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
public:
	explicit TransactionManager(AttachedDatabase &db);
	virtual ~TransactionManager();

	//! Start a new transaction
	virtual Transaction &StartTransaction(ClientContext &context) = 0;
	//! Commit the given transaction. Returns a non-empty error message on failure.
	virtual ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) = 0;
	//! Rollback the given transaction
	virtual void RollbackTransaction(Transaction &transaction) = 0;

	virtual void Checkpoint(ClientContext &context, bool force = false) = 0;

	static TransactionManager &Get(AttachedDatabase &db);

	virtual bool IsDuckTransactionManager() {
		return false;
	}

	AttachedDatabase &GetDB() {
		return db;
	}

	//! Returns a version number that uniquely characterizes the current catalog snapshot.
	//! If there are transaction-local changes, the version returned is >= TRANSACTION_START, o.w. it is a simple number
	//! starting at 0 that is incremented at each commit that has had catalog changes.
	virtual idx_t GetCatalogVersion(Transaction &transaction) {
		throw duckdb::NotImplementedException("catalog version not supported for this TX manager");
	}

protected:
	//! The attached database
	AttachedDatabase &db;
};

} // namespace duckdb
