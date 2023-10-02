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
	ValidChecker();

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
	//! Set to true if a fatal exception has occurred
	mutex invalidate_lock;
	atomic<bool> is_invalidated;
	string invalidated_msg;
};

} // namespace duckdb
