#include "duckdb/transaction/transaction_context.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {

TransactionContext::TransactionContext(ClientContext &context)
    : context(context), auto_commit(true), current_transaction(nullptr) {
}

TransactionContext::~TransactionContext() {
	if (current_transaction) {
		try {
			Rollback();
		} catch (...) {
		}
	}
}

void TransactionContext::BeginTransaction() {
	if (current_transaction) {
		throw TransactionException("cannot start a transaction within a transaction");
	}
	auto start_timestamp = Timestamp::GetCurrentTimestamp();
	auto catalog_version = Catalog::GetSystemCatalog(context).GetCatalogVersion();
	current_transaction = make_uniq<MetaTransaction>(context, start_timestamp, catalog_version);

	auto &config = DBConfig::GetConfig(context);
	if (config.options.immediate_transaction_mode) {
		// if immediate transaction mode is enabled then start all transactions immediately
		auto databases = DatabaseManager::Get(context).GetDatabases(context);
		for (auto db : databases) {
			current_transaction->GetTransaction(db.get());
		}
	}
}

void TransactionContext::Commit() {
	if (!current_transaction) {
		throw TransactionException("failed to commit: no transaction active");
	}
	auto transaction = std::move(current_transaction);
	ClearTransaction();
	string error = transaction->Commit();
	if (!error.empty()) {
		throw TransactionException("Failed to commit: %s", error);
	}
}

void TransactionContext::SetAutoCommit(bool value) {
	auto_commit = value;
	if (!auto_commit && !current_transaction) {
		BeginTransaction();
	}
}

void TransactionContext::Rollback() {
	if (!current_transaction) {
		throw TransactionException("failed to rollback: no transaction active");
	}
	auto transaction = std::move(current_transaction);
	ClearTransaction();
	transaction->Rollback();
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

} // namespace duckdb
