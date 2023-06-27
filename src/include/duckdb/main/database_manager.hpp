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
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"

namespace duckdb {
class AttachedDatabase;
class Catalog;
class CatalogSet;
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

	void InitializeSystemCatalog();
	//! Get an attached database with the given name
	optional_ptr<AttachedDatabase> GetDatabase(ClientContext &context, const string &name);
	//! Add a new attached database to the database manager
	void AddDatabase(ClientContext &context, unique_ptr<AttachedDatabase> db);
	void DetachDatabase(ClientContext &context, const string &name, OnEntryNotFound if_not_found);
	//! Returns a reference to the system catalog
	Catalog &GetSystemCatalog();
	static const string &GetDefaultDatabase(ClientContext &context);
	void SetDefaultDatabase(ClientContext &context, const string &new_value);

	optional_ptr<AttachedDatabase> GetDatabaseFromPath(ClientContext &context, const string &path);
	vector<reference<AttachedDatabase>> GetDatabases(ClientContext &context);

	transaction_t GetNewQueryNumber() {
		return current_query_number++;
	}
	transaction_t ActiveQueryNumber() const {
		return current_query_number;
	}
	idx_t ModifyCatalog() {
		return catalog_version++;
	}
	bool HasDefaultDatabase() {
		return !default_database.empty();
	}

private:
	//! The system database is a special database that holds system entries (e.g. functions)
	unique_ptr<AttachedDatabase> system;
	//! The set of attached databases
	unique_ptr<CatalogSet> databases;
	//! The global catalog version, incremented whenever anything changes in the catalog
	atomic<idx_t> catalog_version;
	//! The current query number
	atomic<transaction_t> current_query_number;
	//! The current default database
	string default_database;
};

} // namespace duckdb
