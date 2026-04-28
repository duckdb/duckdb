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
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"

namespace duckdb {

class ClientContext;
class MetaTransaction;
class Transaction;
class TransactionManager;
struct ClaimParticipantResult;

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
	//! Join an existing transaction by id. The id has the form "<connection_id>/<database_name>"
	//! as produced by duckdb_share_transaction(). The current transaction must be explicit (not
	//! auto-commit) and must not yet hold a transaction against the named database. Throws
	//! TransactionException on any precondition or lookup failure.
	void JoinTransaction(const string &transaction_id);
	//! Foreign-safe entry point for claiming a participant slot in this connection's active
	//! transaction. Atomically: takes the meta-state lock, checks current_transaction, and
	//! delegates to MetaTransaction::TryClaimParticipant. Returns an empty result if there is
	//! no active transaction or the lookup fails. Safe to call from a different connection's
	//! thread (used by JOIN TRANSACTION) — the lock prevents a torn read against a concurrent
	//! Commit/Rollback that is moving out current_transaction on this connection's own thread.
	ClaimParticipantResult TryForeignClaimParticipant(const string &db_name);

	void SetAutoCommit(bool value);
	bool IsAutoCommit() const {
		return auto_commit;
	}

	void SetReadOnly();

	idx_t GetActiveQuery();
	void ResetActiveQuery();
	void SetActiveQuery(transaction_t query_number);

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

	//! Serializes mutation of `current_transaction` (BeginTransaction / Commit / Rollback /
	//! ClearTransaction) against foreign reads via TryForeignClaimParticipant. Local accesses
	//! within the owning connection's own thread are not racy against each other (the
	//! ClientContext's context_lock serializes them), so most local reads do NOT take this
	//! lock — only the writers do.
	mutable mutex meta_lock;
	unique_ptr<MetaTransaction> current_transaction;

	TransactionContext(const TransactionContext &) = delete;
};

} // namespace duckdb
