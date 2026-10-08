//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/os_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {
class DatabaseInstance;

//! The database's door to the operating system: every access to process-level state goes through here,
//! so settings such as enable_external_access apply to all of it
class OSUtil {
public:
	explicit OSUtil(DatabaseInstance &db);
	virtual ~OSUtil() = default;

	DUCKDB_API static OSUtil &Get(DatabaseInstance &db);

public:
	//! Reads an environment variable on behalf of the user. Returns false if it is unset, and throws a
	//! PermissionException when enable_external_access is disabled.
	DUCKDB_API virtual bool GetEnv(const string &name, string &value);
	//! The value of an environment variable, or an empty string if it is unset; same restrictions as above
	DUCKDB_API string GetEnv(const string &name);

	//! Reads a variable the engine (or the embedding application) configures itself from, such as HOME or TZ.
	//! Not subject to enable_external_access, so only known configuration variables are accepted: the engine's
	//! own (see IsConfigurationEnv) and those the embedder registered. Any other name is a programming error.
	DUCKDB_API virtual bool GetEnvUnrestricted(const string &name, string &value);
	//! The value of such a variable, or an empty string if it is unset
	DUCKDB_API string GetEnvUnrestricted(const string &name);

	//! Whether the engine itself configures from this variable
	DUCKDB_API static bool IsConfigurationEnv(const string &name);
	//! Lets the embedding application (the shell, a host program) read its own configuration variables
	//! through GetEnvUnrestricted. Not exposed to extensions.
	DUCKDB_API void RegisterConfigurationEnv(const string &name);

protected:
	//! Reads a variable from the process environment, without any checks
	DUCKDB_API virtual bool ReadEnv(const string &name, string &value);

protected:
	DatabaseInstance &db;
	mutex lock;
	//! Variables registered through RegisterConfigurationEnv
	unordered_set<string> configuration_env;
};

} // namespace duckdb
