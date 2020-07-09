//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/config.hpp"
#include "duckdb/main/extension.hpp"

namespace duckdb {
class StorageManager;
class Catalog;
class TransactionManager;
class ConnectionManager;
class FileSystem;
class TaskScheduler;

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class Connection;
class DuckDB {
public:
	DuckDB(const char *path = nullptr, DBConfig *config = nullptr);
	DuckDB(const string &path, DBConfig *config = nullptr);

	~DuckDB();

    DBConfig config;

	unique_ptr<StorageManager> storage;
	unique_ptr<Catalog> catalog;
	unique_ptr<TransactionManager> transaction_manager;
	unique_ptr<TaskScheduler> scheduler;
	unique_ptr<ConnectionManager> connection_manager;
public:
	template <class T> void LoadExtension() {
		T extension;
		extension.Load(*this);
	}

	FileSystem &GetFileSystem();
private:
	void Configure(DBConfig &config);
};

} // namespace duckdb
