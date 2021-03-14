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

struct PragmaFunctionPragma {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaTableInfo {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaLastProfilingOutput {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaDetailedProfilingOutput {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaExpressionsProfilingOutput {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SQLiteMaster {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaVersion {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaDatabaseList {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaDatabaseSize {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
