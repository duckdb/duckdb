//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/system_functions.hpp
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

struct PragmaStorageInfo {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaLastProfilingOutput {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaDetailedProfilingOutput {
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

struct DuckDBSchemasFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBColumnsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBConstraintsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBDependenciesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBFunctionsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBKeywordsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBIndexesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBSequencesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBSettingsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBTablesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBViewsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBMatViewsFun {
    static void RegisterFunction(BuiltinFunctions &set);
};

struct TestAllTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
