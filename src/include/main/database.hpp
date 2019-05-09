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

enum AccessMode { UNDEFINED, READ_ONLY, READ_WRITE }; // TODO AUTOMATIC

// this is optional and only used in tests at the moment
struct DBConfig {
	AccessMode access_mode = AccessMode::UNDEFINED;
	unique_ptr<FileSystem> file_system;
};

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class Connection;
class DuckDB {
public:
	DuckDB(const char *path = nullptr, DBConfig *config = nullptr);
	DuckDB(const string &path, DBConfig *config = nullptr);

	~DuckDB();

	unique_ptr<FileSystem> file_system;
	unique_ptr<StorageManager> storage;
	unique_ptr<Catalog> catalog;
	unique_ptr<TransactionManager> transaction_manager;
	unique_ptr<ConnectionManager> connection_manager;

	AccessMode access_mode = AccessMode::READ_WRITE;
};

} // namespace duckdb
