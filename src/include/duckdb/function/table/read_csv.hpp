//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/read_csv.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct ReadCSVTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
