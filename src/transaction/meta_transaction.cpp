#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"

namespace duckdb {

MetaTransaction::MetaTransaction(ClientContext &context_p, timestamp_t start_timestamp_p, idx_t catalog_version_p)
    : context(context_p), start_timestamp(start_timestamp_p), catalog_version(catalog_version_p), read_only(true),
      active_query(MAXIMUM_QUERY_ID), modified_database(nullptr) {
}

MetaTransaction &MetaTransaction::Get(ClientContext &context) {
	return context.transaction.ActiveTransaction();
}

ValidChecker &ValidChecker::Get(MetaTransaction &transaction) {
	return transaction.transaction_validity;
}

Transaction &Transaction::Get(ClientContext &context, AttachedDatabase &db) {
	auto &meta_transaction = MetaTransaction::Get(context);
	return meta_transaction.GetTransaction(db);
}

Transaction &MetaTransaction::GetTransaction(AttachedDatabase &db) {
	auto entry = transactions.find(db);
	if (entry == transactions.end()) {
		auto &new_transaction = db.GetTransactionManager().StartTransaction(context);
		new_transaction.active_query = active_query;
		all_transactions.push_back(db);
		transactions.insert(make_pair(reference<AttachedDatabase>(db), reference<Transaction>(new_transaction)));
		return new_transaction;
	} else {
		D_ASSERT(entry->second.get().active_query == active_query);
		return entry->second;
	}
}

void MetaTransaction::RemoveTransaction(AttachedDatabase &db) {
	auto entry = transactions.find(db);
	if (entry == transactions.end()) {
		throw InternalException("MetaTransaction::RemoveTransaction called but meta transaction did not have a "
		                        "transaction for this database");
	}
	transactions.erase(entry);
	for (idx_t i = 0; i < all_transactions.size(); i++) {
		auto &db_entry = all_transactions[i];
		if (RefersToSameObject(db_entry.get(), db)) {
			all_transactions.erase(all_transactions.begin() + i);
			break;
		}
	}
}

Transaction &Transaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog.GetAttached());
}

ErrorData MetaTransaction::Commit() {
	ErrorData error;
	// commit transactions in reverse order
	for (idx_t i = all_transactions.size(); i > 0; i--) {
		auto &db = all_transactions[i - 1].get();
		auto entry = transactions.find(db);
		if (entry == transactions.end()) {
			throw InternalException("Could not find transaction corresponding to database in MetaTransaction");
		}
		auto &transaction_manager = db.GetTransactionManager();
		auto &transaction = entry->second.get();
		if (!error.HasError()) {
			// commit
			error = transaction_manager.CommitTransaction(context, transaction);
		} else {
			// we have encountered an error previously - roll back subsequent entries
			transaction_manager.RollbackTransaction(transaction);
		}
	}
	return error;
}

void MetaTransaction::Rollback() {
	// rollback transactions in reverse order
	for (idx_t i = all_transactions.size(); i > 0; i--) {
		auto &db = all_transactions[i - 1].get();
		auto &transaction_manager = db.GetTransactionManager();
		auto entry = transactions.find(db);
		D_ASSERT(entry != transactions.end());
		auto &transaction = entry->second.get();
		transaction_manager.RollbackTransaction(transaction);
	}
}

idx_t MetaTransaction::GetActiveQuery() {
	return active_query;
}

void MetaTransaction::SetActiveQuery(transaction_t query_number) {
	active_query = query_number;
	for (auto &entry : transactions) {
		entry.second.get().active_query = query_number;
	}
}

void MetaTransaction::ModifyDatabase(AttachedDatabase &db) {
	if (db.IsSystem() || db.IsTemporary()) {
		// we can always modify the system and temp databases
		return;
	}
	if (!modified_database) {
		modified_database = &db;
		return;
	}
	if (&db != modified_database.get()) {
		throw TransactionException(
		    "Attempting to write to database \"%s\" in a transaction that has already modified database \"%s\" - a "
		    "single transaction can only write to a single attached database.",
		    db.GetName(), modified_database->GetName());
	}
}

} // namespace duckdb
