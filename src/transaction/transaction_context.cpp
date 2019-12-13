#include "duckdb/transaction/transaction_context.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

using namespace duckdb;
using namespace std;

TransactionContext::~TransactionContext() {
	if (is_invalidated) {
		return;
	}
	if (current_transaction) {
		try {
			Rollback();
		} catch (...) {
		}
	}
}

void TransactionContext::BeginTransaction() {
	if (current_transaction) {
		throw TransactionException("Transaction is already running!");
	}
	current_transaction = transaction_manager.StartTransaction();
}

void TransactionContext::Commit() {
	auto transaction = current_transaction;
	if (!transaction) {
		throw TransactionException("No transaction is currently active - cannot commit!");
	}
	SetAutoCommit(true);
	current_transaction = nullptr;
	string error = transaction_manager.CommitTransaction(transaction);
	if (!error.empty()) {
		throw TransactionException("Failed to commit: %s", error.c_str());
	}
}

void TransactionContext::Rollback() {
	auto transaction = current_transaction;
	if (!transaction) {
		throw TransactionException("No transaction is currently active - cannot rollback!");
	}
	SetAutoCommit(true);
	current_transaction = nullptr;
	transaction_manager.RollbackTransaction(transaction);
}
