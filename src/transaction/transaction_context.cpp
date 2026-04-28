#include "duckdb/transaction/transaction_context.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

TransactionContext::TransactionContext(ClientContext &context)
    : context(context), auto_commit(true), current_transaction(nullptr) {
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
	{
		// meta_lock serializes the publish of current_transaction against foreign readers
		// (TryForeignClaimParticipant), so they never observe a torn pointer.
		lock_guard<mutex> guard(meta_lock);
		current_transaction = make_uniq<MetaTransaction>(context, start_timestamp, global_transaction_id);
	}

	// Notify any registered state of transaction begin
	for (auto &state : context.registered_state->States()) {
		state->TransactionBegin(*current_transaction, context);
	}
}

void TransactionContext::Commit() {
	if (!current_transaction) {
		throw TransactionException("failed to commit: no transaction active");
	}
	unique_ptr<MetaTransaction> transaction;
	{
		// Move out under meta_lock so foreign readers see either "active" or "no active txn",
		// never a half-moved unique_ptr. The MetaTransaction object itself stays alive in the
		// local `transaction` variable for the rest of this function.
		lock_guard<mutex> guard(meta_lock);
		transaction = std::move(current_transaction);
	}
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
	unique_ptr<MetaTransaction> transaction;
	{
		lock_guard<mutex> guard(meta_lock);
		transaction = std::move(current_transaction);
	}
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
	lock_guard<mutex> guard(meta_lock);
	current_transaction = nullptr;
}

ClaimParticipantResult TransactionContext::TryForeignClaimParticipant(const string &db_name) {
	lock_guard<mutex> guard(meta_lock);
	if (!current_transaction) {
		return ClaimParticipantResult();
	}
	return current_transaction->TryClaimParticipant(db_name);
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

void TransactionContext::JoinTransaction(const string &transaction_id) {
	if (auto_commit) {
		throw TransactionException(
		    "JOIN TRANSACTION can only be used inside an explicit transaction (use BEGIN first)");
	}
	if (!current_transaction) {
		throw TransactionException("JOIN TRANSACTION called without an active transaction");
	}
	// Defensive validation. The id is user input; reject pathological inputs early so we don't
	// echo arbitrary data into error messages or hand it to downstream lookups.
	constexpr idx_t MAX_TRANSACTION_ID_LEN = 1024;
	if (transaction_id.empty()) {
		throw TransactionException("JOIN TRANSACTION: transaction id must be non-empty");
	}
	if (transaction_id.size() > MAX_TRANSACTION_ID_LEN) {
		throw TransactionException("JOIN TRANSACTION: transaction id exceeds maximum length of %llu bytes",
		                           static_cast<uint64_t>(MAX_TRANSACTION_ID_LEN));
	}
	for (auto c : transaction_id) {
		auto uc = static_cast<unsigned char>(c);
		if (uc < 0x20 || uc == 0x7F) {
			throw TransactionException(
			    "JOIN TRANSACTION: transaction id contains an invalid control character (0x%02x)",
			    static_cast<uint32_t>(uc));
		}
	}
	// Split on the LAST '/' so database names containing slashes round-trip correctly.
	auto slash = transaction_id.rfind('/');
	if (slash == string::npos || slash == 0 || slash == transaction_id.size() - 1) {
		throw TransactionException("Invalid transaction id '%s': expected '<connection_id>/<database_name>'",
		                           transaction_id);
	}
	auto conn_id_str = transaction_id.substr(0, slash);
	auto db_name = transaction_id.substr(slash + 1);
	uint64_t conn_id_raw;
	if (!TryCast::Operation<string_t, uint64_t>(string_t(conn_id_str), conn_id_raw)) {
		throw TransactionException("Invalid transaction id '%s': connection id is not a number", transaction_id);
	}
	auto conn_id = static_cast<connection_t>(conn_id_raw);
	if (conn_id == context.GetConnectionId()) {
		throw TransactionException("Cannot join a transaction owned by the same connection");
	}
	auto &connection_manager = ConnectionManager::Get(context);
	auto owner_context = connection_manager.FindByConnectionId(conn_id);
	if (!owner_context) {
		throw TransactionException("Invalid transaction id '%s': no live connection with id %llu", transaction_id,
		                           static_cast<uint64_t>(conn_id));
	}
	// Single foreign call that atomically: takes the owner's TransactionContext meta_lock,
	// checks current_transaction is non-null, and runs TryClaimParticipant under owner_meta.lock.
	// This closes two race windows at once:
	//   - the cross-connection torn-read of current_transaction during a concurrent
	//     Commit/Rollback that is std::move'ing it,
	//   - the TOCTOU between lookup and TryAddParticipant that an owner-side DETACH could slip
	//     into.
	// On success the foreign DuckTransaction is pinned by the bumped participant_count, so the
	// owner can finalize its MetaTransaction freely from this point on.
	auto claim = owner_context->transaction.TryForeignClaimParticipant(db_name);
	if (!claim.transaction) {
		throw TransactionException(
		    "Invalid transaction id '%s': owner has no live transaction for database '%s' "
		    "(no active transaction, database not yet touched, finalized, or not a DuckDB database)",
		    transaction_id, db_name);
	}
	// Acquire the foreign DuckTransaction's statement lock BEFORE touching its state. This
	// blocks until any owner query in flight on this transaction completes. With BeginQuery
	// holding the same lock for the duration of every query, no other connection can be
	// mutating the transaction's LocalStorage / UndoBuffer / active_query while we import.
	unique_lock<mutex> foreign_guard;
	try {
		foreign_guard = claim.transaction->LockStatement();
	} catch (...) {
		claim.transaction->CancelParticipation(context);
		throw;
	}
	try {
		current_transaction->ImportTransaction(*claim.database, *claim.transaction);
	} catch (...) {
		claim.transaction->CancelParticipation(context);
		throw;
	}
	// Stash the lock on the active query so it stays held for the rest of this statement and
	// is released at end-of-query along with the BeginQuery-acquired guards.
	context.RegisterSharedStatementGuard(std::move(foreign_guard));
}

void TransactionContext::SetActiveQuery(transaction_t query_number) {
	if (!current_transaction) {
		throw InternalException("SetActiveQuery called without active transaction");
	}
	current_transaction->SetActiveQuery(query_number);
}

} // namespace duckdb
