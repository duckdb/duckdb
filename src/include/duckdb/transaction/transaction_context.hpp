//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"

namespace duckdb {

class ClientContext;
class MetaTransaction;
class Transaction;
class TransactionManager;

//! The transaction context keeps track of all the information relating to the
//! current transaction
class TransactionContext {
public:
	explicit TransactionContext(ClientContext &context);
	~TransactionContext();

	MetaTransaction &ActiveTransaction() {
		if (!HasActiveTransaction()) {
			throw InternalException("TransactionContext::ActiveTransaction called without active transaction");
		}
		return *current_transaction;
	}

	bool HasActiveTransaction() const {
		return current_transaction.get();
	}

	void BeginTransaction();
	void Commit();
	void Rollback(optional_ptr<ErrorData>);
	void ClearTransaction();
	void SetAutocheckpointError(ErrorData error);

	void SetAutoCommit(bool value);
	bool IsAutoCommit() const {
		return auto_commit;
	}

	void SetReadOnly();

	//! Makes the current transaction joinable by other connections, returns its identifier
	string ShareTransaction();
	//! Joins the shared transaction with the given identifier (only outside of a transaction)
	void JoinTransaction(const string &share_id);
	//! The current transaction if it was shared under the given identifier, otherwise null.
	//! Safe to call from other connections.
	shared_ptr<MetaTransaction> GetSharedTransaction(const string &share_id);
	//! Whether the current transaction is shared with other connections. Safe to call from other threads.
	bool IsInSharedTransaction();
	//! Continues in the transaction active on `source` (only outside of a transaction). The connection then cannot
	//! end the transaction: it stays in it until it is closed, and the transaction cannot commit while it is open.
	void InheritTransaction(ClientContext &source);
	//! Whether the connection inherited its transaction (see InheritTransaction)
	bool IsInherited() const {
		return inherited;
	}

	idx_t GetActiveQuery();
	void ResetActiveQuery();
	void SetActiveQuery(transaction_t query_number);

	void SetInvalidationPolicy(TransactionInvalidationPolicy new_invalidation_policy);
	TransactionInvalidationPolicy GetInvalidationPolicy() {
		return invalidation_policy;
	};
	void SetAutoRollback(bool new_auto_rollback) {
		auto_rollback = new_auto_rollback;
	};
	bool GetAutoRollback() {
		return auto_rollback;
	};

private:
	ClientContext &context;
	bool auto_commit;
	TransactionInvalidationPolicy invalidation_policy = TransactionInvalidationPolicy::STANDARD_POLICY;
	bool auto_rollback = false;
	//! Whether the current transaction was inherited from another connection
	bool inherited = false;

	//! Guards replacing current_transaction against reads from other connections (GetSharedTransaction)
	mutex current_transaction_lock;
	shared_ptr<MetaTransaction> current_transaction;
	//! The query number of the query currently running on this connection (MAXIMUM_QUERY_ID if none)
	atomic<transaction_t> active_query_id = MAXIMUM_QUERY_ID;
	ErrorData autocheckpoint_error;

	void SetCurrentTransaction(shared_ptr<MetaTransaction> transaction);
	shared_ptr<MetaTransaction> TakeCurrentTransaction();
	shared_ptr<MetaTransaction> GetCurrentTransaction();
	void RollbackTransaction(const shared_ptr<MetaTransaction> &transaction, optional_ptr<ErrorData> error);
	//! Joins `transaction` and continues in it, discarding the current transaction (if any)
	void EnterTransaction(shared_ptr<MetaTransaction> transaction, bool inherit, const string &not_active_error,
	                      const string &aborted_error);

	TransactionContext(const TransactionContext &) = delete;
};

} // namespace duckdb
