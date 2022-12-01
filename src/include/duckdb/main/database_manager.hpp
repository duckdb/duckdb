//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class AttachedDatabase;
class Catalog;
class ClientContext;
class DatabaseInstance;

//! The DatabaseManager is a class that sits at the root of all attached databases
class DatabaseManager {
	friend class Catalog;

public:
	explicit DatabaseManager(DatabaseInstance &db);
	~DatabaseManager();

public:
	static DatabaseManager &Get(DatabaseInstance &db);
	static DatabaseManager &Get(ClientContext &db);
	static DatabaseManager &Get(AttachedDatabase &db);

	//! Get an attached database with the given name
	AttachedDatabase *GetDatabase(const string &name);
	//! Add a new attached database to the database manager
	void AddDatabase(unique_ptr<AttachedDatabase> db);
	//! Returns a reference to the system catalog
	Catalog &GetSystemCatalog();
	AttachedDatabase &GetDefaultDatabase();

	vector<AttachedDatabase *> GetDatabases();

	transaction_t GetNewQueryNumber() {
		return current_query_number++;
	}
	transaction_t ActiveQueryNumber() const {
		return current_query_number;
	}

private:
	//! The lock controlling access to the databases
	mutex manager_lock;
	//! The set of attached databases
	case_insensitive_map_t<unique_ptr<AttachedDatabase>> databases;
	//! The system catalog is a special catalog that holds system entries (e.g. functions)
	unique_ptr<Catalog> system_catalog;
	//! The global catalog version, incremented whenever anything changes in the catalog
	atomic<idx_t> catalog_version;
	//! The current query number
	atomic<transaction_t> current_query_number;
};

} // namespace duckdb
