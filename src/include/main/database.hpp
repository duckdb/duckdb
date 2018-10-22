//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// main/database.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace duckdb {

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class DuckDB {
  public:
	DuckDB(const char *path = nullptr);
	DuckDB(const std::string &path) : DuckDB(path.c_str()) {
	}

	StorageManager storage;
	Catalog catalog;
	TransactionManager transaction_manager;
};

} // namespace duckdb
