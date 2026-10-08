//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/os_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {
class DatabaseInstance;

//! The database's door to the operating system: every access to process-level state goes through here,
//! so settings such as enable_external_access apply to all of it
class OSUtil {
public:
	//! Created when the database is configured, with the configuration variables the embedder declared in the
	//! DBConfig handed to the DuckDB constructor; nothing loaded into the database afterwards can widen the set
	OSUtil(DatabaseInstance &db, unordered_set<string> configuration_env);

	DUCKDB_API static OSUtil &Get(DatabaseInstance &db);

public:
	//! Reads an environment variable on behalf of the user. Returns false if it is unset, and throws a
	//! PermissionException when enable_external_access is disabled.
	DUCKDB_API bool GetEnv(const string &name, string &value);
	//! The value of an environment variable, or an empty string if it is unset; same restrictions as above
	DUCKDB_API string GetEnv(const string &name);

	//! Reads a variable the engine or the embedding application configures itself from, such as HOME or TZ.
	//! Not subject to enable_external_access, so only known configuration variables are accepted: the engine's
	//! own (see IsConfigurationEnv) and those the embedder declared in DBConfigOptions::configuration_env.
	//! Any other name is a programming error.
	DUCKDB_API bool GetEnvUnrestricted(const string &name, string &value);
	//! The value of such a variable, or an empty string if it is unset
	DUCKDB_API string GetEnvUnrestricted(const string &name);

	//! Whether the engine itself configures from this variable
	DUCKDB_API static bool IsConfigurationEnv(const string &name);
	//! Reads one of the engine's own configuration variables when no database exists yet, for example while a
	//! DBConfig is being built. Any other name is a programming error.
	DUCKDB_API static bool GetConfigurationEnv(const string &name, string &value);
	//! The value of such a variable, or an empty string if it is unset
	DUCKDB_API static string GetConfigurationEnv(const string &name);

private:
	//! Reads a variable from the process environment, without any checks
	static bool ReadEnv(const string &name, string &value);

private:
	DatabaseInstance &db;
	//! Variables the embedding application declared as its configuration
	const unordered_set<string> configuration_env;
};

} // namespace duckdb
