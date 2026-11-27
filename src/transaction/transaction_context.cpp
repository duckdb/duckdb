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
	auto transaction = TakeCurrentTransaction();
	ClearTransaction();
	bool commit_result = true;
	if (!LeaveTransaction(*transaction, true, commit_result)) {
		// other connections still participate in the transaction, the last one to leave ends it
		for (auto &state : context.GetContext().GetRegisteredState().States()) {
			state->TransactionCommit(*transaction, context);
		}
		return;
	}
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
	auto transaction = TakeCurrentTransaction();
	ClearTransaction();
	bool commit_result = false;
	if (!LeaveTransaction(*transaction, false, commit_result)) {
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

bool TransactionContext::LeaveTransaction(MetaTransaction &transaction, bool commit, bool &commit_result) {
	auto result = transaction.Leave(commit);
	commit_result = result.commit;
	return result.last;
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
	if (!source || source->IsFinished()) {
		throw TransactionException("invalid transaction identifier: \"%s\"", share_id);
	}
	if (ValidChecker::IsInvalidated(*source)) {
		throw TransactionException("cannot join transaction \"%s\": it was aborted", share_id);
	}
	if (!source->Join()) {
		throw TransactionException("invalid transaction identifier: \"%s\"", share_id);
	}
	// discard the transaction that was started for this statement and continue in the shared one
	auto previous = TakeCurrentTransaction();
	SetCurrentTransaction(std::move(source));
	auto_commit = false;
	previous->MarkFinished();
	try {
		RollbackTransaction(previous, nullptr);
	} catch (std::exception &) {
		// the discarded transaction is empty, a failure to end it must not undo the join
	}
	NotifyTransactionBegin();
}

void TransactionContext::JoinTransaction(TransactionContext &source) {
	if (!auto_commit || HasActiveTransaction()) {
		throw InternalException("JoinTransaction: the connection already has a transaction");
	}
	shared_ptr<MetaTransaction> transaction;
	{
		lock_guard<mutex> guard(source.current_transaction_lock);
		transaction = source.current_transaction;
	}
	if (!transaction || !transaction->Join()) {
		throw TransactionException("cannot join the transaction of a connection that has no active transaction");
	}
	SetCurrentTransaction(std::move(transaction));
	auto_commit = false;
	NotifyTransactionBegin();
}

void TransactionContext::NotifyTransactionBegin() {
	for (auto &state : context.GetContext().GetRegisteredState().States()) {
		state->TransactionBegin(*current_transaction, context);
	}
}

void TransactionContext::ClearTransaction() {
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
