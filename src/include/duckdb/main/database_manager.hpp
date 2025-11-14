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
#include "duckdb/main/database_file_path_manager.hpp"

namespace duckdb {
class AttachedDatabase;
class Catalog;
class CatalogEntryRetriever;
class CatalogSet;
class ClientContext;
class DatabaseInstance;
class TaskScheduler;
struct AttachOptions;
struct AlterInfo;

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
	//! Finalize starting up the system
	void FinalizeStartup();
	//! Get an attached database by its name
	optional_ptr<AttachedDatabase> GetDatabase(ClientContext &context, const string &name);
	shared_ptr<AttachedDatabase> GetDatabase(const string &name);
	//! Attach a new database
	shared_ptr<AttachedDatabase> AttachDatabase(ClientContext &context, AttachInfo &info, AttachOptions &options);

	//! Detach an existing database
	void DetachDatabase(ClientContext &context, const string &name, OnEntryNotFound if_not_found);
	//! Alter operation dispatcher
	void Alter(ClientContext &context, AlterInfo &info);
	//! Rollback the attach of a database
	shared_ptr<AttachedDatabase> DetachInternal(const string &name);
	//! Returns a reference to the system catalog
	Catalog &GetSystemCatalog();

	static const string &GetDefaultDatabase(ClientContext &context);
	void SetDefaultDatabase(ClientContext &context, const string &new_value);

	//! Inserts a path to name mapping to the database paths map
	InsertDatabasePathResult InsertDatabasePath(const AttachInfo &info, AttachOptions &options);

	//! Returns the database type. This might require checking the header of the file, in which case the file handle is
	//! necessary. We can only grab the file handle, if it is not yet held, even for uncommitted changes. Thus, we have
	//! to lock for this operation.
	void GetDatabaseType(ClientContext &context, AttachInfo &info, const DBConfig &config, AttachOptions &options);
	//! Scans the catalog set and adds each committed database entry, and each database entry of the current
	//! transaction, to a vector holding AttachedDatabase references
	vector<shared_ptr<AttachedDatabase>> GetDatabases(ClientContext &context,
	                                                  const optional_idx max_db_count = optional_idx());
	//! Scans the catalog set and returns each committed database entry
	vector<shared_ptr<AttachedDatabase>> GetDatabases();
	//! Returns the approximate count of attached databases.
	idx_t ApproxDatabaseCount();
	//! Removes all databases from the catalog set. This is necessary for the database instance's destructor,
	//! as the database manager has to be alive when destroying the catalog set objects.
	void ResetDatabases(unique_ptr<TaskScheduler> &scheduler);

	transaction_t GetNewQueryNumber() {
		return current_query_number++;
	}
	transaction_t ActiveQueryNumber() const {
		return current_query_number;
	}
	transaction_t GetNewTransactionNumber() {
		return current_transaction_id++;
	}
	transaction_t ActiveTransactionNumber() const {
		return current_transaction_id;
	}
	idx_t NextOid() {
		return next_oid++;
	}
	bool HasDefaultDatabase() {
		return !default_database.empty();
	}
	//! Gets a list of all attached database paths
	vector<string> GetAttachedDatabasePaths();

	shared_ptr<AttachedDatabase> GetDatabaseInternal(const lock_guard<mutex> &, const string &name);

private:
	optional_ptr<AttachedDatabase> FinalizeAttach(ClientContext &context, AttachInfo &info,
	                                              shared_ptr<AttachedDatabase> database);

private:
	//! The system database is a special database that holds system entries (e.g. functions)
	shared_ptr<AttachedDatabase> system;
	//! Lock for databases
	mutex databases_lock;
	//! The set of attached databases
	case_insensitive_map_t<shared_ptr<AttachedDatabase>> databases;
	//! The next object id handed out by the NextOid method
	atomic<idx_t> next_oid;
	//! The current query number
	atomic<transaction_t> current_query_number;
	//! The current transaction number
	atomic<transaction_t> current_transaction_id;
	//! The current default database
	string default_database;
	//! Manager for ensuring we never open the same database file twice in the same program
	shared_ptr<DatabaseFilePathManager> path_manager;

private:
	//! Rename an existing database
	void RenameDatabase(ClientContext &context, const string &old_name, const string &new_name,
	                    OnEntryNotFound if_not_found);
};

} // namespace duckdb
