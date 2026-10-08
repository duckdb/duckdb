//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/os_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
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
	//! Not subject to enable_external_access: use it only for configuration, never for data a query asked for.
	DUCKDB_API virtual bool GetEnvUnrestricted(const string &name, string &value);
	//! The value of such a variable, or an empty string if it is unset
	DUCKDB_API string GetEnvUnrestricted(const string &name);

protected:
	DatabaseInstance &db;
};

} // namespace duckdb
