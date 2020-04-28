//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/sqlite_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct PragmaCollations {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaTableInfo {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SQLiteMaster {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
