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

	idx_t UNCOMMITTED_CATALOG_VERSION_START = 4611686018427388000ULL; // similar approach to TRANSACTION_ID_START

	virtual idx_t GetCatalogVersion(Transaction &transaction) {
		throw duckdb::NotImplementedException("catalog version not supported for this TX manager");
	}

protected:
	//! The attached database
	AttachedDatabase &db;
};

} // namespace duckdb
