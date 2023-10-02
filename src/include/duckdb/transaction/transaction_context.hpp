//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

class ClientContext;
class MetaTransaction;
class Transaction;
class TransactionManager;

//! The transaction context keeps track of all the information relating to the
//! current transaction
class TransactionContext {
public:
	TransactionContext(ClientContext &context);
	~TransactionContext();

	MetaTransaction &ActiveTransaction() {
		if (!current_transaction) {
			throw InternalException("TransactionContext::ActiveTransaction called without active transaction");
		}
		return *current_transaction;
	}

	bool HasActiveTransaction() {
		return !!current_transaction;
	}

	void BeginTransaction();
	void Commit();
	void Rollback();
	void ClearTransaction();

	void SetAutoCommit(bool value);
	bool IsAutoCommit() {
		return auto_commit;
	}

	idx_t GetActiveQuery();
	void ResetActiveQuery();
	void SetActiveQuery(transaction_t query_number);

private:
	ClientContext &context;
	bool auto_commit;

	unique_ptr<MetaTransaction> current_transaction;

	TransactionContext(const TransactionContext &) = delete;
};

} // namespace duckdb
