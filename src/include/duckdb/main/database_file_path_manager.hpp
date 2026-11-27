//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database_file_path_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {
struct AttachInfo;
struct AttachOptions;
struct StoredDatabasePath;
class DatabaseManager;

enum class InsertDatabasePathResult { SUCCESS, ALREADY_EXISTS };

struct DatabasePathInfo {
	DatabasePathInfo(DatabaseManager &manager, const Identifier &name_p, AccessMode access_mode);

	Identifier name;
	AccessMode access_mode;
	reference_map_t<DatabaseManager, identifier_map_t<idx_t>> attached_databases;
	idx_t reference_count = 1;

	bool IsAttachedIn(DatabaseManager &manager, const Identifier &alias) const;
	void Attach(DatabaseManager &manager, const Identifier &alias);
	void Detach(DatabaseManager &manager, const Identifier &alias);
	Identifier GetAttachedName(DatabaseManager &manager) const;
};

//! The DatabaseFilePathManager is used to ensure we only ever open a single database file once
class DatabaseFilePathManager {
public:
	idx_t ApproxDatabaseCount() const;
	InsertDatabasePathResult InsertDatabasePath(DatabaseManager &manager, const string &path, const Identifier &name,
	                                            OnCreateConflict on_conflict, AttachOptions &options);
	//! Erase a database path - indicating we are done with using it
	void EraseDatabasePath(StoredDatabasePath &stored_path);
	//! Called when a database is detached, but before it is fully finished being used
	void DetachDatabase(StoredDatabasePath &stored_path);
	//! Update the tracked alias of an attached database
	void RenameDatabasePath(StoredDatabasePath &stored_path, const Identifier &new_name);
	//! Whether the (canonicalized) path is in use by an attached database
	bool IsDatabasePath(const string &path) const;

private:
	//! The lock to add entries to the db_paths map
	mutable mutex db_paths_lock;
	//! A set containing all attached database path
	//! This allows to attach many databases efficiently, and to avoid attaching the
	//! same file path twice
#if defined(_WIN32) || defined(__APPLE__)
	case_insensitive_map_t<DatabasePathInfo> db_paths;
#else  // !(_WIN32 or __APPLE__)
	unordered_map<string, DatabasePathInfo> db_paths;
#endif // _WIN32 or __APPLE__
};

} // namespace duckdb
