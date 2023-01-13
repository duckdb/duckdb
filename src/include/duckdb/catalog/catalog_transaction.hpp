//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class Catalog;
class ClientContext;
class DatabaseInstance;
class Transaction;

struct CatalogTransaction {
	CatalogTransaction(Catalog &catalog, ClientContext &context);
	CatalogTransaction(DatabaseInstance &db, transaction_t transaction_id_p, transaction_t start_time_p);

	DatabaseInstance *db;
	ClientContext *context;
	Transaction *transaction;
	transaction_t transaction_id;
	transaction_t start_time;

	ClientContext &GetContext();
};

} // namespace duckdb
