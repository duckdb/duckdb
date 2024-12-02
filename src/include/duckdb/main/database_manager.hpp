//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

namespace duckdb {
class AttachedDatabase;
class Catalog;
class CatalogEntryRetriever;
class CatalogSet;
class ClientContext;
class DatabaseInstance;
class TaskScheduler;
struct AttachOptions;

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

	//! Initializes the system catalog of the attached SYSTEM_DATABASE.
	void InitializeSystemCatalog();
	//! Get an attached database by its name
	optional_ptr<AttachedDatabase> GetDatabase(ClientContext &context, const string &name);
	//! Attach a new database
	optional_ptr<AttachedDatabase> AttachDatabase(ClientContext &context, const AttachInfo &info,
	                                              const AttachOptions &options);
	//! Detach an existing database
	void DetachDatabase(ClientContext &context, const string &name, OnEntryNotFound if_not_found);
	//! Returns a reference to the system catalog
	Catalog &GetSystemCatalog();

	static const string &GetDefaultDatabase(ClientContext &context);
	void SetDefaultDatabase(ClientContext &context, const string &new_value);

	//! Inserts a path to name mapping to the database paths map
	void InsertDatabasePath(ClientContext &context, const string &path, const string &name);
	//! Erases a path from the database paths map
	void EraseDatabasePath(const string &path);

	//! Returns the database type. This might require checking the header of the file, in which case the file handle is
	//! necessary. We can only grab the file handle, if it is not yet held, even for uncommitted changes. Thus, we have
	//! to lock for this operation.
	void GetDatabaseType(ClientContext &context, AttachInfo &info, const DBConfig &config, AttachOptions &options);
	//! Scans the catalog set and adds each committed database entry, and each database entry of the current
	//! transaction, to a vector holding AttachedDatabase references
	vector<reference<AttachedDatabase>> GetDatabases(ClientContext &context);
	//! Removes all databases from the catalog set. This is necessary for the database instance's destructor,
	//! as the database manager has to be alive when destroying the catalog set objects.
	void ResetDatabases(unique_ptr<TaskScheduler> &scheduler);

	transaction_t GetNewQueryNumber() {
		return current_query_number++;
	}
	transaction_t ActiveQueryNumber() const {
		return current_query_number;
	}
	idx_t NextOid() {
		return next_oid++;
	}
	bool HasDefaultDatabase() {
		return !default_database.empty();
	}
	//! Gets a list of all attached database paths
	vector<string> GetAttachedDatabasePaths();

private:
	//! Returns a database with a specified path
	optional_ptr<AttachedDatabase> GetDatabaseFromPath(ClientContext &context, const string &path);
	void CheckPathConflict(ClientContext &context, const string &path);

private:
	//! The system database is a special database that holds system entries (e.g. functions)
	unique_ptr<AttachedDatabase> system;
	//! The set of attached databases
	unique_ptr<CatalogSet> databases;
	//! The next object id handed out by the NextOid method
	atomic<idx_t> next_oid;
	//! The current query number
	atomic<transaction_t> current_query_number;
	//! The current default database
	string default_database;

	//! The lock to add entries to the database path map
	mutex db_paths_lock;
	//! A set containing all attached database path
	//! This allows to attach many databases efficiently, and to avoid attaching the
	//! same file path twice
	case_insensitive_set_t db_paths;
};

} // namespace duckdb
