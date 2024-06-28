//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/pragma/pragma_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct PragmaQueries {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaFunctions {
	static void RegisterFunction(BuiltinFunctions &set);
};

string PragmaShowTables();
string PragmaShowTablesExpanded();
string PragmaShowDatabases();
string PragmaShow(const string &table_name);

} // namespace duckdb
