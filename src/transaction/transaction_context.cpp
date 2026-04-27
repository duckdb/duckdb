#include "duckdb/transaction/transaction_context.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

TransactionContext::TransactionContext(ClientContext &context)
    : context(context), auto_commit(true), invalidation_policy(TransactionInvalidationPolicy::STANDARD_POLICY),
      auto_rollback(false), current_transaction(nullptr) {
}

TransactionContext::~TransactionContext() {
	if (current_transaction) {
		try {
			Rollback(nullptr);
		} catch (std::exception &ex) {
			ErrorData data(ex);
			try {
				DUCKDB_LOG_ERROR(context, "TransactionContext::~TransactionContext()\t\t" + data.Message());
			} catch (...) { // NOLINT
			}
		} catch (...) { // NOLINT
		}
	}
}

void TransactionContext::BeginTransaction() {
	if (current_transaction) {
		throw TransactionException("cannot start a transaction within a transaction");
	}
	auto start_timestamp = Timestamp::GetCurrentTimestamp();
	auto global_transaction_id = context.db->GetDatabaseManager().GetNewTransactionNumber();
	current_transaction = make_shared_ptr<MetaTransaction>(context, start_timestamp, global_transaction_id);

	// Notify any registered state of transaction begin
	for (auto &state : context.registered_state->States()) {
		state->TransactionBegin(*current_transaction, context);
	}
}

void TransactionContext::AdoptSharedTransaction(shared_ptr<MetaTransaction> shared) {
	// SET TRANSACTION SNAPSHOT 'id' must be issued inside a freshly-begun transaction
	// (BEGIN; SET TRANSACTION SNAPSHOT 'id'; ...). This matches Postgres semantics: the
	// snapshot replaces the characteristics of the current transaction, which must not
	// have done any work yet.
	if (auto_commit) {
		throw TransactionException("SET TRANSACTION SNAPSHOT must be called inside a transaction (run BEGIN first)");
	}
	if (!current_transaction) {
		throw InternalException("AdoptSharedTransaction: no current transaction");
	}
	if (!shared) {
		throw InternalException("AdoptSharedTransaction called with null transaction");
	}
	if (shared->IsForceDetached()) {
		throw TransactionException("the shared transaction has been rolled back by its owner");
	}
	if (current_transaction->IsShared()) {
		throw TransactionException("this connection is already attached to a shared transaction");
	}
	if (!current_transaction->OpenedTransactions().empty()) {
		throw TransactionException("SET TRANSACTION SNAPSHOT must be the first statement of the transaction");
	}
	// Discard the empty current MetaTransaction (it has no per-database transactions
	// yet, so nothing to roll back) and install the shared one.
	current_transaction->Finalize();
	current_transaction.reset();
	shared->AttachParticipant();
	current_transaction = std::move(shared);
	// auto_commit stays false: we remain inside an explicit transaction (the shared one).
}

bool TransactionContext::IsSharedParticipant() const {
	if (!current_transaction) {
		return false;
	}
	return current_transaction->IsShared() && !current_transaction->IsOwner(context);
}

bool TransactionContext::IsSharedOwner() const {
	if (!current_transaction) {
		return false;
	}
	return current_transaction->IsShared() && current_transaction->IsOwner(context);
}

void TransactionContext::Commit() {
	if (!current_transaction) {
		throw TransactionException("failed to commit: no transaction active");
	}
	// Participant detach: a participant's COMMIT does not finalize the shared
	// transaction. It only releases this context's reference. The originating
	// (owner) context is the only one that can actually commit/rollback. We still
	// notify registered_state so per-context observers (DebugClientContextState,
	// extension hooks) see the participant's transaction-begin matched by an end.
	if (current_transaction->IsShared() && !current_transaction->IsOwner(context)) {
		auto shared = std::move(current_transaction);
		ClearTransaction();
		for (auto &state : context.registered_state->States()) {
			state->TransactionCommit(*shared, context);
		}
		shared->DetachParticipant();
		return;
	}
	// Owner path: if the transaction is shared, we must wait for participants to detach
	// before finalizing. Release the per-meta statement lock first — the lock exists to
	// serialize concurrent storage writes during query execution, but the wait phase is
	// pure synchronization and holding it would deadlock against participants trying to
	// acquire the same lock at the start of their queries (including the ROLLBACK that
	// finally lets them detach). Also mark the meta as committing, which causes the
	// snapshot registry to reject any new participant imports while we wait — without
	// this, a late attach could re-grow `participant_count` and starve the wait.
	if (current_transaction->IsShared()) {
		current_transaction->MarkCommitting();
		context.ReleaseSharedStatementGuardForWait();
		if (!current_transaction->WaitForParticipantsToDetach(context)) {
			throw TransactionException(
			    "shared transaction COMMIT was interrupted while waiting for participants to detach");
		}
	}
	auto transaction = std::move(current_transaction);
	ClearTransaction();
	auto error = transaction->Commit();
	// Notify any registered state of transaction commit
	if (error.HasError()) {
		for (auto const &s : context.registered_state->States()) {
			s->TransactionRollback(*transaction, context, error);
		}
		if (Exception::InvalidatesDatabase(error.Type()) || error.Type() == ExceptionType::INTERNAL) {
			// throw fatal / internal exceptions directly
			error.Throw();
		}
		throw TransactionException("Failed to commit: %s", error.RawMessage());
	}
	for (auto &state : context.registered_state->States()) {
		state->TransactionCommit(*transaction, context);
	}
	transaction->Finalize();
}

void TransactionContext::SetAutoCommit(bool value) {
	auto_commit = value;
	if (!auto_commit && !current_transaction) {
		BeginTransaction();
	}
}

void TransactionContext::SetReadOnly() {
	current_transaction->SetReadOnly();
}

void TransactionContext::Rollback(optional_ptr<ErrorData> error) {
	if (!current_transaction) {
		throw TransactionException("failed to rollback: no transaction active");
	}
	// Participant detach: a participant's ROLLBACK does not roll back the shared
	// transaction. It only releases this context's reference. Notify registered_state
	// so per-context observers see the participant's transaction-begin matched by
	// an end.
	if (current_transaction->IsShared() && !current_transaction->IsOwner(context)) {
		auto shared = std::move(current_transaction);
		ClearTransaction();
		for (auto const &state : context.registered_state->States()) {
			state->TransactionRollback(*shared, context, error);
		}
		shared->DetachParticipant();
		return;
	}
	// Owner path: if shared, signal participants that the transaction is being rolled
	// back, then wait for them to detach before destroying any per-database state. This
	// keeps the per-db DuckTransactions alive while participants still hold a reference
	// to the shared meta — the alternative (rolling back immediately and leaving
	// participants with dangling references) would be a use-after-free.
	// Release the per-meta statement lock before waiting (see Commit() above for the
	// reasoning; same deadlock hazard applies).
	if (current_transaction->IsShared()) {
		current_transaction->ForceDetach();
		context.ReleaseSharedStatementGuardForWait();
		if (!current_transaction->WaitForParticipantsToDetach(context)) {
			throw TransactionException(
			    "shared transaction ROLLBACK was interrupted while waiting for participants to detach");
		}
	}
	auto transaction = std::move(current_transaction);
	ClearTransaction();
	context.client_data->profiler->Reset();

	ErrorData rollback_error;
	try {
		transaction->Rollback();
	} catch (std::exception &ex) {
		rollback_error = ErrorData(ex);
	}
	// Notify any registered state of transaction rollback
	for (auto const &s : context.registered_state->States()) {
		s->TransactionRollback(*transaction, context, error);
	}
	if (rollback_error.HasError()) {
		rollback_error.Throw();
	}
	transaction->Finalize();
}

void TransactionContext::ClearTransaction() {
	SetAutoCommit(true);
	current_transaction = nullptr;
}

idx_t TransactionContext::GetActiveQuery() {
	if (!current_transaction) {
		throw InternalException("GetActiveQuery called without active transaction");
	}
	return current_transaction->GetActiveQuery();
}

void TransactionContext::ResetActiveQuery() {
	if (current_transaction) {
		SetActiveQuery(MAXIMUM_QUERY_ID);
	}
}

void TransactionContext::SetActiveQuery(transaction_t query_number) {
	if (!current_transaction) {
		throw InternalException("SetActiveQuery called without active transaction");
	}
	current_transaction->SetActiveQuery(query_number);
}

void TransactionContext::BeginQuery(unique_lock<mutex> &shared_statement_guard_out, transaction_t query_number) {
	if (current_transaction && current_transaction->IsShared()) {
		shared_statement_guard_out = current_transaction->LockSharedStatement();
	}
	if (current_transaction) {
		current_transaction->SetActiveQuery(query_number);
	}
}

} // namespace duckdb
