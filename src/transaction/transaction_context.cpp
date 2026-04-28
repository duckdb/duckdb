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
	current_transaction = make_uniq<MetaTransaction>(context, start_timestamp, global_transaction_id);

	// Notify any registered state of transaction begin
	for (auto &state : context.registered_state->States()) {
		state->TransactionBegin(*current_transaction, context);
	}
}

void TransactionContext::Commit() {
	if (!current_transaction) {
		throw TransactionException("failed to commit: no transaction active");
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

void TransactionContext::ImportSnapshot(const string &snapshot_id) {
	if (auto_commit) {
		throw TransactionException(
		    "SET TRANSACTION SNAPSHOT can only be used inside an explicit transaction (use BEGIN first)");
	}
	if (!current_transaction) {
		throw TransactionException("SET TRANSACTION SNAPSHOT called without an active transaction");
	}
	auto slash = snapshot_id.find('/');
	if (slash == string::npos || slash == 0 || slash == snapshot_id.size() - 1) {
		throw TransactionException("Invalid transaction snapshot id '%s': expected '<connection_id>/<database_name>'",
		                           snapshot_id);
	}
	auto conn_id_str = snapshot_id.substr(0, slash);
	auto db_name = snapshot_id.substr(slash + 1);
	uint64_t conn_id_raw;
	if (!TryCast::Operation<string_t, uint64_t>(string_t(conn_id_str), conn_id_raw)) {
		throw TransactionException("Invalid transaction snapshot id '%s': connection id is not a number", snapshot_id);
	}
	auto conn_id = static_cast<connection_t>(conn_id_raw);
	if (conn_id == context.GetConnectionId()) {
		throw TransactionException("Cannot import a transaction snapshot from the same connection");
	}
	auto &connection_manager = ConnectionManager::Get(context);
	auto owner_context = connection_manager.FindByConnectionId(conn_id);
	if (!owner_context) {
		throw TransactionException("Invalid transaction snapshot id '%s': no live connection with id %llu", snapshot_id,
		                           static_cast<uint64_t>(conn_id));
	}
	if (!owner_context->transaction.HasActiveTransaction()) {
		throw TransactionException(
		    "Invalid transaction snapshot id '%s': owner connection is not currently in a transaction", snapshot_id);
	}
	auto &owner_meta = owner_context->transaction.ActiveTransaction();
	optional_ptr<AttachedDatabase> matching_db;
	for (auto &db_ref : owner_meta.OpenedTransactions()) {
		auto &db = db_ref.get();
		if (StringUtil::CIEquals(db.GetName(), db_name)) {
			matching_db = &db;
			break;
		}
	}
	if (!matching_db) {
		throw TransactionException(
		    "Invalid transaction snapshot id '%s': owner has not yet started a transaction against database '%s'",
		    snapshot_id, db_name);
	}
	auto owner_transaction = owner_meta.TryGetTransaction(*matching_db);
	if (!owner_transaction) {
		throw TransactionException("Invalid transaction snapshot id '%s': owner has no transaction for database '%s'",
		                           snapshot_id, db_name);
	}
	if (!owner_transaction->IsDuckTransaction()) {
		throw TransactionException("Database '%s' does not support shared transactions (not a DuckDB-managed database)",
		                           db_name);
	}
	auto &duck_transaction = owner_transaction->Cast<DuckTransaction>();
	if (!duck_transaction.TryAddParticipant()) {
		throw TransactionException("Invalid transaction snapshot id '%s': transaction has already been finalized",
		                           snapshot_id);
	}
	try {
		current_transaction->ImportTransaction(*matching_db, duck_transaction);
	} catch (...) {
		duck_transaction.CancelParticipation();
		throw;
	}
}

void TransactionContext::SetActiveQuery(transaction_t query_number) {
	if (!current_transaction) {
		throw InternalException("SetActiveQuery called without active transaction");
	}
	current_transaction->SetActiveQuery(query_number);
}

} // namespace duckdb
