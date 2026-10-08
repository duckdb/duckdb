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
	//! Reads an environment variable. Returns false if it is unset, or if reading it is not allowed.
	//! Variables the engine needs to configure itself (see IsSafeEnv) can always be read; everything
	//! else requires enable_external_access.
	DUCKDB_API virtual bool TryGetEnv(const string &name, string &value);
	//! The value of an environment variable, or an empty string if it is unset or not allowed
	DUCKDB_API string GetEnv(const string &name);

	//! Whether a variable can be read even when external access is disabled
	DUCKDB_API static bool IsSafeEnv(const string &name);
	//! Marks a variable the embedding application uses to configure itself as readable regardless of external access
	DUCKDB_API void AddSafeEnv(const string &name);

protected:
	//! Reads a variable from the process environment, without any policy
	DUCKDB_API virtual bool ReadEnv(const string &name, string &value);

protected:
	DatabaseInstance &db;
	mutex lock;
	//! Variables declared safe through AddSafeEnv
	unordered_set<string> safe_env;
};

} // namespace duckdb
