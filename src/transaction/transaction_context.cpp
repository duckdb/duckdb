#include "duckdb/transaction/transaction_context.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

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
	current_transaction = transaction_manager.StartTransaction(context);
}

void TransactionContext::Commit() {
	if (!current_transaction) {
		throw TransactionException("failed to commit: no transaction active");
	}
	auto transaction = current_transaction;
	SetAutoCommit(true);
	current_transaction = nullptr;
	string error = transaction_manager.CommitTransaction(context, transaction);
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
	auto transaction = current_transaction;
	ClearTransaction();
	transaction_manager.RollbackTransaction(transaction);
}

void TransactionContext::ClearTransaction() {
	SetAutoCommit(true);
	current_transaction = nullptr;
}

} // namespace duckdb
