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
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog_entry.hpp"

namespace duckdb {
class Catalog;
class DatabaseInstance;
class StorageManager;
class TransactionManager;

enum class BuiltInDatabaseType {
	NOT_BUILT_IN,
	SYSTEM_DATABASE,
	TEMP_DATABASE,
};

//! The AttachedDatabase represents an attached database instance
class AttachedDatabase : public CatalogEntry {
public:
	//! Create the built-in system attached database (without storage)
	explicit AttachedDatabase(DatabaseInstance &db, BuiltInDatabaseType type = BuiltInDatabaseType::SYSTEM_DATABASE);
	//! Create an attached database instance with the specified name and storage
	AttachedDatabase(DatabaseInstance &db, Catalog &catalog, string name, string file_path, AccessMode access_mode);
	~AttachedDatabase();

	void Initialize();

	StorageManager &GetStorageManager();
	Catalog &GetCatalog();
	TransactionManager &GetTransactionManager();
	DatabaseInstance &GetDatabase() {
		return db;
	}
	const string &GetName() const {
		return name;
	}
	bool IsSystem() const;
	bool IsTemporary() const;

	static string ExtractDatabaseName(const string &dbpath);

private:
	DatabaseInstance &db;
	unique_ptr<StorageManager> storage;
	unique_ptr<Catalog> catalog;
	unique_ptr<TransactionManager> transaction_manager;
	BuiltInDatabaseType type;
};

} // namespace duckdb
