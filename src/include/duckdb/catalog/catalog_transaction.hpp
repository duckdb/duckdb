//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class Catalog;
class ClientContext;
class DatabaseInstance;
class Transaction;

struct CatalogTransaction {
	CatalogTransaction(Catalog &catalog, ClientContext &context);
	CatalogTransaction(DatabaseInstance &db, transaction_t transaction_id_p, transaction_t start_time_p);

	optional_ptr<DatabaseInstance> db;
	optional_ptr<ClientContext> context;
	optional_ptr<Transaction> transaction;
	transaction_t transaction_id;
	transaction_t start_time;

	bool HasContext() const {
		return context;
	}
	ClientContext &GetContext();

	static CatalogTransaction GetSystemCatalogTransaction(ClientContext &context);
	static CatalogTransaction GetSystemTransaction(DatabaseInstance &db);
};

} // namespace duckdb
