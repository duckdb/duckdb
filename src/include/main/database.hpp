//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.hpp"
#include "main/connection_manager.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace duckdb {

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class Connection;
class DuckDB {
public:
	DuckDB(const char *path = nullptr, bool read_only = false);
	DuckDB(const string &path, bool read_only = false) : DuckDB(path.c_str(), read_only) {
	}

	StorageManager storage;
	Catalog catalog;
	TransactionManager transaction_manager;
	ConnectionManager connection_manager;
	bool read_only;
};

} // namespace duckdb
