//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/information_schema_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct InformationSchemaTables {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct InformationSchemaColumns {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
