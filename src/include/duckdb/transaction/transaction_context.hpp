//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class Transaction;
class TransactionManager;

//! The transaction context keeps track of all the information relating to the
//! current transaction
class TransactionContext {
public:
	TransactionContext(TransactionManager &transaction_manager)
	    : transaction_manager(transaction_manager), auto_commit(true), is_invalidated(false),
	      current_transaction(nullptr) {
	}
	~TransactionContext();

	Transaction &ActiveTransaction() {
		assert(current_transaction);
		return *current_transaction;
	}

	bool HasActiveTransaction() {
		return !!current_transaction;
	}

	void RecordQuery(string query);
	void BeginTransaction();
	void Commit();
	void Rollback();

	void SetAutoCommit(bool value) {
		auto_commit = value;
	}
	bool IsAutoCommit() {
		return auto_commit;
	}

	void Invalidate() {
		is_invalidated = true;
	}

private:
	TransactionManager &transaction_manager;
	bool auto_commit;
	bool is_invalidated;

	Transaction *current_transaction;

	TransactionContext(const TransactionContext &) = delete;
};

} // namespace duckdb
