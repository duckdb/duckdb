//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/attached_database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class Catalog;
class DatabaseInstance;
class StorageManager;
class TransactionManager;

//! The AttachedDatabase represents an attached database instance
class AttachedDatabase {
public:
	explicit AttachedDatabase(DatabaseInstance &db);
	~AttachedDatabase();

	void Initialize();

	StorageManager &GetStorageManager();
	Catalog &GetCatalog();
	TransactionManager &GetTransactionManager();

private:
	static string ExtractDatabaseName(const string &dbpath);

private:
	DatabaseInstance &db;
	//! The database name
	string name;
	unique_ptr<StorageManager> storage;
	unique_ptr<Catalog> catalog;
	unique_ptr<TransactionManager> transaction_manager;
};

} // namespace duckdb
