#include "duckdb/transaction/transaction_context.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/shared_transaction_lock.hpp"

namespace duckdb {

TransactionContext::TransactionContext(ClientContext &context)
    : context(context), auto_commit(true), invalidation_policy(TransactionInvalidationPolicy::STANDARD_POLICY),
      auto_rollback(false), current_transaction(nullptr) {
}

TransactionContext::~TransactionContext() {
	if (current_transaction) {
		try {
			// Destruction cannot wait for participants; hand the transaction over instead.
			Rollback(nullptr, true);
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
	current_transaction = make_uniq<MetaTransaction>(context, start_timestamp, global_transaction_id);

	// Notify any registered state of transaction begin
	for (auto &state : context.registered_state->States()) {
		state->TransactionBegin(*current_transaction, context);
	}
}

void TransactionContext::SetInvalidationPolicy(TransactionInvalidationPolicy new_invalidation_policy) {
	if (new_invalidation_policy == TransactionInvalidationPolicy::STANDARD_POLICY) {
		// if no policy is specified explicitly use the default one from the settings
		new_invalidation_policy = Settings::Get<DefaultTransactionInvalidationPolicySetting>(context);
	}
	invalidation_policy = new_invalidation_policy;
}

void TransactionContext::SetAutocheckpointError(ErrorData error) {
	autocheckpoint_error = std::move(error);
}

void TransactionContext::Commit() {
	if (!current_transaction) {
		throw TransactionException("failed to commit: no transaction active");
	}
	autocheckpoint_error = ErrorData();
	// Hold the statement lock across the commit: it can end a shared transaction, and no participant may be reading it.
	auto guard = context.LockSharedTransactionForFinalize(*current_transaction);
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
	if (autocheckpoint_error.HasError()) {
		auto err = std::move(autocheckpoint_error);
		autocheckpoint_error = ErrorData();
		err.Throw();
	}
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

void TransactionContext::Rollback(optional_ptr<ErrorData> error, bool allow_hand_off) {
	if (!current_transaction) {
		throw TransactionException("failed to rollback: no transaction active");
	}
	// Hold the statement lock across the rollback: it can end a shared transaction, and no participant may be
	// reading it. Automatic rollback of a failed statement reaches this after the query released its own guard.
	bool hand_off = false;
	auto guard = context.LockSharedTransactionForFinalize(*current_transaction, allow_hand_off, &hand_off);
	auto transaction = std::move(current_transaction);
	ClearTransaction();
	context.client_data->profiler->Reset();

	ErrorData rollback_error;
	try {
		transaction->Rollback(hand_off);
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

void TransactionContext::SetTransactionSnapshot(const string &snapshot_id) {
	if (auto_commit || !current_transaction) {
		throw TransactionException("SET TRANSACTION SNAPSHOT can only be used inside an explicit transaction");
	}
	if (snapshot_id.empty()) {
		throw TransactionException("SET TRANSACTION SNAPSHOT requires a non-empty snapshot id");
	}
	if (ValidChecker::IsInvalidated(*current_transaction)) {
		throw TransactionException("Cannot set the transaction snapshot of an invalidated transaction");
	}
	if (current_transaction->SharedDatabase()) {
		throw TransactionException("Cannot set the transaction snapshot: this connection already takes part in a "
		                           "shared transaction for database %s",
		                           current_transaction->SharedDatabase()->GetName());
	}

	auto &database_manager = DatabaseManager::Get(context);
	auto database = database_manager.GetSharedTransactionDatabase(snapshot_id);
	if (!database) {
		throw TransactionException("Snapshot is no longer available");
	}
	if (ValidChecker::IsInvalidated(*database)) {
		throw TransactionException("Cannot set the transaction snapshot: %s",
		                           ValidChecker::InvalidatedMessage(*database));
	}
	auto &transaction_manager = database->GetTransactionManager();
	if (!transaction_manager.IsDuckTransactionManager()) {
		throw TransactionException("Database %s does not support transaction snapshots", database->GetName());
	}
	auto &duck_manager = transaction_manager.Cast<DuckTransactionManager>();
	// Hold the statement lock before looking the transaction up so its owner cannot end it underneath us.
	context.GuardSharedTransaction(duck_manager.GetSharedTransactionState(snapshot_id)->GetStatementLock(),
	                               SharedTransactionGuardMode::ACQUIRE_SHARED);
	auto &transaction = duck_manager.JoinTransaction(snapshot_id);
	try {
		current_transaction->AdoptTransaction(*database, transaction);
	} catch (...) {
		transaction.GetSharedState()->AbandonJoin();
		throw;
	}
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

} // namespace duckdb
