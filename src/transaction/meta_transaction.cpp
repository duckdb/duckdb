#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

MetaTransaction::MetaTransaction(ClientContext &context_p, timestamp_t start_timestamp_p, idx_t catalog_version_p)
    : context(context_p), start_timestamp(start_timestamp_p), catalog_version(catalog_version_p), read_only(true),
      active_query(MAXIMUM_QUERY_ID) {
}

MetaTransaction &MetaTransaction::Get(ClientContext &context) {
	return context.transaction.ActiveTransaction();
}

ValidChecker &ValidChecker::Get(MetaTransaction &transaction) {
	return transaction.transaction_validity;
}

Transaction &Transaction::Get(ClientContext &context, AttachedDatabase &db) {
	auto &meta_transaction = MetaTransaction::Get(context);
	auto entry = meta_transaction.transactions.find(&db);
	if (entry == meta_transaction.transactions.end()) {
		auto new_transaction = db.GetTransactionManager().StartTransaction(context);
		new_transaction->active_query = meta_transaction.active_query;
		meta_transaction.transactions[&db] = new_transaction;
		return *new_transaction;
	} else {
		D_ASSERT(entry->second->active_query == meta_transaction.active_query);
		return *entry->second;
	}
}

Transaction &Transaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog.GetAttached());
}

string MetaTransaction::Commit() {
	string error;
	for (auto &entry : transactions) {
		auto db = entry.first;
		auto &transaction_manager = db->GetTransactionManager();
		auto transaction = entry.second;
		if (error.empty()) {
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
	for (auto &entry : transactions) {
		auto db = entry.first;
		auto &transaction_manager = db->GetTransactionManager();
		auto transaction = entry.second;
		transaction_manager.RollbackTransaction(transaction);
	}
}

idx_t MetaTransaction::GetActiveQuery() {
	return active_query;
}

void MetaTransaction::SetActiveQuery(transaction_t query_number) {
	active_query = query_number;
	for (auto &entry : transactions) {
		entry.second->active_query = query_number;
	}
}

} // namespace duckdb
