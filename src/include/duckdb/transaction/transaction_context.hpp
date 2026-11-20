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

	//! Starts executing a write in the current transaction, waiting for the other connections that share it.
	//! Returns false if this connection already is executing a write.
	bool BeginWrite();
	void EndWrite();

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

	//! Guards replacing current_transaction against reads from other connections (GetSharedTransaction)
	mutex current_transaction_lock;
	shared_ptr<MetaTransaction> current_transaction;
	//! The transaction in which this connection is executing a write
	shared_ptr<MetaTransaction> write_transaction;
	//! The query number of the query currently running on this connection (MAXIMUM_QUERY_ID if none)
	atomic<transaction_t> active_query_id = MAXIMUM_QUERY_ID;
	ErrorData autocheckpoint_error;

	//! Leaves the transaction. Returns true if the caller is the last participant and has to end it
	bool LeaveTransaction(MetaTransaction &transaction, bool commit, bool &commit_result);
	void SetCurrentTransaction(shared_ptr<MetaTransaction> transaction);
	shared_ptr<MetaTransaction> TakeCurrentTransaction();
	void RollbackTransaction(shared_ptr<MetaTransaction> transaction, optional_ptr<ErrorData> error);

	TransactionContext(const TransactionContext &) = delete;
};

//! Ends the write that the connection is executing when it goes out of scope
struct WriteEndGuard {
	WriteEndGuard(TransactionContext &transaction_p, bool owns_p) : transaction(transaction_p), owns(owns_p) {
	}
	~WriteEndGuard() {
		if (owns) {
			transaction.EndWrite();
		}
	}

	TransactionContext &transaction;
	bool owns;
};

} // namespace duckdb
