//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

namespace duckdb {
class StorageManager;
class Catalog;
class TransactionManager;
class ConnectionManager;
class FileSystem;

// this is optional and only used in tests at the moment
struct DBConfig {
	unique_ptr<FileSystem> file_system;
	// TODO add other things from below here?
};

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class Connection;
class DuckDB {
public:
	DuckDB(const char *path = nullptr, bool read_only = false, DBConfig *config = nullptr);
	DuckDB(const string &path, bool read_only = false, DBConfig *config = nullptr);

	~DuckDB();

	unique_ptr<FileSystem> file_system;
	unique_ptr<StorageManager> storage;
	unique_ptr<Catalog> catalog;
	unique_ptr<TransactionManager> transaction_manager;
	unique_ptr<ConnectionManager> connection_manager;

	bool read_only;
};

} // namespace duckdb
