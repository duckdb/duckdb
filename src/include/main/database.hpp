//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/file_system.hpp"

namespace duckdb {
class StorageManager;
class Catalog;
class TransactionManager;
class ConnectionManager;
class FileSystem;

enum AccessMode { UNDEFINED, READ_ONLY, READ_WRITE }; // TODO AUTOMATIC

// this is optional and only used in tests at the moment
struct DBConfig {
	friend class DuckDB;
	friend class StorageManager;
public:
	~DBConfig();

	AccessMode access_mode = AccessMode::UNDEFINED;
	unique_ptr<FileSystem> file_system;
	// checkpoint when WAL reaches this size
	index_t checkpoint_wal_size = 1 << 20;
private:
	// FIXME: don't set this as a user: used internally (only for now)
	bool checkpoint_only = false;
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

	AccessMode access_mode;
	bool checkpoint_only;
	index_t checkpoint_wal_size;

private:
	void Configure(DBConfig &config);
};

} // namespace duckdb
