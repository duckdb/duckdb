//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database_file_path_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"

namespace duckdb {
struct AttachInfo;
struct AttachOptions;

enum class InsertDatabasePathResult { SUCCESS, ALREADY_EXISTS };

//! The DatabaseFilePathManager is used to ensure we only ever open a single database file once
class DatabaseFilePathManager {
public:
	idx_t ApproxDatabaseCount() const;
	InsertDatabasePathResult InsertDatabasePath(const string &path, const string &name, OnCreateConflict on_conflict,
	                                            AttachOptions &options);
	void EraseDatabasePath(const string &path);

private:
	//! The lock to add entries to the database path map
	mutable mutex db_paths_lock;
	//! A set containing all attached database path
	//! This allows to attach many databases efficiently, and to avoid attaching the
	//! same file path twice
	case_insensitive_map_t<string> db_paths_to_name;
};

} // namespace duckdb
