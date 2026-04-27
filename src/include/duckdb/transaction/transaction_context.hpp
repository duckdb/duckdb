//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/mutex.hpp"
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
		if (!current_transaction) {
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

	//! Adopt an existing shared transaction as a participant. Must be called inside an
	//! explicit transaction (after BEGIN) that has not yet performed any work. The
	//! current (empty) transaction is discarded and replaced with the shared one.
	void AdoptSharedTransaction(shared_ptr<MetaTransaction> shared);
	//! Whether the current transaction is shared and this context is a participant
	//! (i.e. not the owner).
	bool IsSharedParticipant() const;
	//! Whether the current transaction is shared and this context is the owner.
	bool IsSharedOwner() const;
	//! Access the underlying shared_ptr (for export).
	shared_ptr<MetaTransaction> GetSharedTransaction() const {
		return current_transaction;
	}

	void SetAutoCommit(bool value);
	bool IsAutoCommit() const {
		return auto_commit;
	}

	void SetReadOnly();

	idx_t GetActiveQuery();
	void ResetActiveQuery();
	void SetActiveQuery(transaction_t query_number);
	//! Begin a query: if the active transaction is shared, acquire the per-meta
	//! statement lock into `shared_statement_guard_out` *before* writing the new
	//! active-query number to the meta. The order matters because
	//! `MetaTransaction::SetActiveQuery` writes to per-database `Transaction::active_query`
	//! fields shared across owner + participants, and concurrent writes there race.
	//! Centralizing both operations here means callers cannot get the order wrong.
	void BeginQuery(unique_lock<mutex> &shared_statement_guard_out, transaction_t query_number);

	//! For an owner of a shared transaction, run the wait-for-participants phase of an
	//! explicit COMMIT/ROLLBACK *on the client thread* before the executor dispatches
	//! the corresponding `PhysicalTransaction` operator on a TaskScheduler worker.
	//!
	//! The per-meta statement guard is a `unique_lock<std::mutex>` whose ownership is
	//! tied to the thread that locked it (the client thread, in `BeginQuery`). Releasing
	//! it from the operator running on a worker thread is wrong-thread mutex unlock —
	//! UB and a TSAN failure. Doing the release + wait here, on the client thread, keeps
	//! the lock thread-confined. The matching `Commit()`/`Rollback()` calls invoked from
	//! the operator then see the lock already released and the participant count already
	//! drained, so their wait phase is a no-op.
	void PrepareSharedOwnerFinalization(TransactionType type);

	void SetInvalidationPolicy(TransactionInvalidationPolicy new_invalidation_policy) {
		invalidation_policy = new_invalidation_policy;
	};
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
	TransactionInvalidationPolicy invalidation_policy;
	bool auto_rollback;

	shared_ptr<MetaTransaction> current_transaction;

	TransactionContext(const TransactionContext &) = delete;
};

} // namespace duckdb
