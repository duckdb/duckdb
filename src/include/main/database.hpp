//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace duckdb {

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class DuckDBConnection;
class DuckDB {
public:
	DuckDB(const char *path = nullptr);
	DuckDB(const string &path) : DuckDB(path.c_str()) {
	}

	StorageManager storage;
	Catalog catalog;
	TransactionManager transaction_manager;
	// TODO this might get slow
	vector<DuckDBConnection *> connections;
};

} // namespace duckdb
