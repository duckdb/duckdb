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

struct DatabasePathInfo {
	explicit DatabasePathInfo(string name_p) : name(std::move(name_p)) {
	}

	string name;
};

//! The DatabaseFilePathManager is used to ensure we only ever open a single database file once
class DatabaseFilePathManager {
public:
	idx_t ApproxDatabaseCount() const;
	InsertDatabasePathResult InsertDatabasePath(const string &path, const string &name, OnCreateConflict on_conflict,
	                                            AttachOptions &options);
	//! Erase a database path - indicating we are done with using it
	void EraseDatabasePath(const string &path);

private:
	//! The lock to add entries to the db_paths map
	mutable mutex db_paths_lock;
	//! A set containing all attached database paths mapped to their attached database name
	case_insensitive_map_t<DatabasePathInfo> db_paths;
};

} // namespace duckdb
