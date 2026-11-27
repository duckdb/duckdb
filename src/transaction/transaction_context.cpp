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
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/valid_checker.hpp"

namespace duckdb {

TransactionContext::TransactionContext(ClientContext &context)
    : context(context), auto_commit(true), invalidation_policy(TransactionInvalidationPolicy::STANDARD_POLICY),
      auto_rollback(false), current_transaction(nullptr) {
}

void TransactionContext::SetCurrentTransaction(shared_ptr<MetaTransaction> transaction) {
	lock_guard<mutex> guard(current_transaction_lock);
	current_transaction = std::move(transaction);
}

shared_ptr<MetaTransaction> TransactionContext::TakeCurrentTransaction() {
	lock_guard<mutex> guard(current_transaction_lock);
	return std::move(current_transaction);
}

shared_ptr<MetaTransaction> TransactionContext::GetCurrentTransaction() {
	lock_guard<mutex> guard(current_transaction_lock);
	return current_transaction;
}

shared_ptr<MetaTransaction> TransactionContext::GetSharedTransaction(const string &share_id) {
	lock_guard<mutex> guard(current_transaction_lock);
	// a transaction that was never shared has an empty share id, which must not match
	if (!share_id.empty() && current_transaction && current_transaction->GetShareId() == share_id) {
		return current_transaction;
	}
	return nullptr;
}

bool TransactionContext::IsInSharedTransaction() {
	lock_guard<mutex> guard(current_transaction_lock);
	return current_transaction && current_transaction->IsShared();
}

TransactionContext::~TransactionContext() {
}

void TransactionContext::BeginTransaction() {
	if (current_transaction) {
		throw TransactionException("cannot start a transaction within a transaction");
	}
	auto start_timestamp = Timestamp::GetCurrentTimestamp();
	auto global_transaction_id = context.db->GetDatabaseManager().GetNewTransactionNumber();
	SetCurrentTransaction(make_shared_ptr<MetaTransaction>(*context.db, context.client_data->temporary_objects,
	                                                       start_timestamp, global_transaction_id));

	// Notify any registered state of transaction begin
	for (auto &state : context.GetContext().GetRegisteredState().States()) {
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
	const bool was_inherited = inherited;
	auto transaction = TakeCurrentTransaction();
	ClearTransaction();
	auto leave = transaction->Leave(true, was_inherited);
	if (!leave.last) {
		if (leave.orphaned) {
			ValidChecker::Invalidate(*transaction, "the transaction was rolled back: it was ended while a connection "
			                                       "that inherited it was still open");
			for (auto const &state : context.GetContext().GetRegisteredState().States()) {
				state->TransactionRollback(*transaction, context, nullptr);
			}
			throw TransactionException(
			    "Failed to commit: a connection that inherited the transaction is still open, it is rolled back");
		}
		// other connections still participate in the transaction, the last one to leave ends it
		for (auto &state : context.GetContext().GetRegisteredState().States()) {
			state->TransactionCommit(*transaction, context);
		}
		return;
	}
	const bool commit_result = leave.commit;
	if (!commit_result) {
		// a participant rolled back: the transaction cannot be committed
		RollbackTransaction(transaction, nullptr);
		throw TransactionException("Failed to commit: a connection sharing the transaction rolled it back");
	}
	auto error = transaction->Commit(context);
	// Notify any registered state of transaction commit
	if (error.HasError()) {
		for (auto const &s : context.GetContext().GetRegisteredState().States()) {
			s->TransactionRollback(*transaction, context, error);
		}
		if (Exception::InvalidatesDatabase(error.Type()) || error.Type() == ExceptionType::INTERNAL) {
			// throw fatal / internal exceptions directly
			error.Throw();
		}
		throw TransactionException("Failed to commit: %s", error.RawMessage());
	}
	for (auto &state : context.GetContext().GetRegisteredState().States()) {
		state->TransactionCommit(*transaction, context);
	}
	transaction->Finalize(context);
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

void TransactionContext::Rollback(optional_ptr<ErrorData> error) {
	if (!current_transaction) {
		throw TransactionException("failed to rollback: no transaction active");
	}
	const bool was_inherited = inherited;
	auto transaction = TakeCurrentTransaction();
	ClearTransaction();
	// a connection that inherited the transaction does not vote: its failures already invalidated the transaction
	auto leave = transaction->Leave(was_inherited, was_inherited);
	if (!leave.last) {
		if (leave.orphaned) {
			ValidChecker::Invalidate(*transaction, "the transaction was rolled back while a connection that "
			                                       "inherited it was still open");
		}
		for (auto &state : context.GetContext().GetRegisteredState().States()) {
			state->TransactionRollback(*transaction, context, error);
		}
		return;
	}
	RollbackTransaction(transaction, error);
}

void TransactionContext::RollbackTransaction(const shared_ptr<MetaTransaction> &transaction,
                                             optional_ptr<ErrorData> error) {
	ErrorData rollback_error;
	try {
		transaction->Rollback(context);
	} catch (std::exception &ex) {
		rollback_error = ErrorData(ex);
	}
	// Notify any registered state of transaction rollback - all of them, also when one of them fails
	for (auto const &s : context.GetContext().GetRegisteredState().States()) {
		try {
			s->TransactionRollback(*transaction, context, error);
		} catch (std::exception &ex) {
			if (!rollback_error.HasError()) {
				rollback_error = ErrorData(ex);
			}
		}
	}
	if (rollback_error.HasError()) {
		rollback_error.Throw();
	}
	transaction->Finalize(context);
}

string TransactionContext::ShareTransaction() {
	if (!HasActiveTransaction() || auto_commit) {
		throw TransactionException("a transaction can only be shared from within a BEGIN TRANSACTION block");
	}
	return current_transaction->ShareTransaction();
}

void TransactionContext::JoinTransaction(const string &share_id) {
	if (!auto_commit) {
		throw TransactionException("cannot join a transaction within a transaction");
	}
	// find a connection that participates in the shared transaction
	shared_ptr<MetaTransaction> source;
	for (auto &connection : ConnectionManager::Get(*context.db).GetConnectionList()) {
		if (connection.get() == &context) {
			continue;
		}
		source = connection->transaction.GetSharedTransaction(share_id);
		if (source) {
			break;
		}
	}
	auto not_active = StringUtil::Format("invalid transaction identifier: \"%s\"", share_id);
	auto aborted = StringUtil::Format("cannot join transaction \"%s\": it was aborted", share_id);
	EnterTransaction(std::move(source), false, not_active, aborted);
}

void TransactionContext::InheritTransaction(ClientContext &source) {
	if (&source == &context) {
		throw InvalidInputException("a connection cannot inherit its own transaction");
	}
	if (source.db != context.db) {
		throw InvalidInputException("cannot inherit the transaction of a connection to another database instance");
	}
	if (!auto_commit || current_transaction) {
		throw TransactionException("cannot inherit a transaction within a transaction");
	}
	// errors invalidate the transaction as they would in the connection it was inherited from
	invalidation_policy = source.transaction.GetInvalidationPolicy();
	EnterTransaction(source.transaction.GetCurrentTransaction(), true,
	                 "cannot inherit the transaction of a connection that has no active transaction",
	                 "cannot inherit the transaction: it was aborted");
}

void TransactionContext::EnterTransaction(shared_ptr<MetaTransaction> transaction, bool inherit,
                                          const string &not_active_error, const string &aborted_error) {
	if (!transaction || transaction->IsFinished()) {
		throw TransactionException(not_active_error);
	}
	if (ValidChecker::IsInvalidated(*transaction)) {
		throw TransactionException(aborted_error);
	}
	if (!transaction->Join(inherit)) {
		throw TransactionException(not_active_error);
	}
	// a JOIN TRANSACTION statement runs in a transaction of its own: discard it and continue in the joined one
	auto previous = TakeCurrentTransaction();
	SetCurrentTransaction(std::move(transaction));
	auto_commit = false;
	inherited = inherit;
	if (previous) {
		previous->MarkFinished();
		try {
			RollbackTransaction(previous, nullptr);
		} catch (std::exception &) {
			// the discarded transaction is empty, a failure to end it must not undo the join
		}
	}
	for (auto &state : context.GetContext().GetRegisteredState().States()) {
		state->TransactionBegin(*current_transaction, context);
	}
}

void TransactionContext::ClearTransaction() {
	inherited = false;
	SetAutoCommit(true);
	SetCurrentTransaction(nullptr);
	active_query_id = MAXIMUM_QUERY_ID;
}

idx_t TransactionContext::GetActiveQuery() {
	if (!current_transaction) {
		throw InternalException("GetActiveQuery called without active transaction");
	}
	return active_query_id;
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
	active_query_id = query_number;
}

} // namespace duckdb
