//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/valid_checker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class DatabaseInstance;
class MetaTransaction;

class ValidChecker {
public:
	enum class Scope { DATABASE, TRANSACTION };
	ValidChecker(DatabaseInstance &db, Scope scope);

	DUCKDB_API static ValidChecker &Get(DatabaseInstance &db);
	DUCKDB_API static ValidChecker &Get(MetaTransaction &transaction);

	DUCKDB_API void Invalidate(string error);
	DUCKDB_API bool IsInvalidated();
	DUCKDB_API string InvalidatedMessage();

	template <class T>
	static bool IsInvalidated(T &o) {
		return Get(o).IsInvalidated();
	}
	template <class T>
	static void Invalidate(T &o, string error) {
		Get(o).Invalidate(std::move(error));
	}

	template <class T>
	static string InvalidatedMessage(T &o) {
		return Get(o).InvalidatedMessage();
	}

private:
	mutex invalidate_lock;
	//! Set to true when encountering a fatal exception.
	atomic<bool> is_invalidated;
	//! The message invalidating the database instance.
	string invalidated_msg;
	//! The database instance.
	DatabaseInstance &db;
	//! Which scope this checker represents — controls whether the
	//! disable_database_invalidation setting gates IsInvalidated.
	const Scope scope;
};

} // namespace duckdb
