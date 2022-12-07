//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/summary.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct SummaryTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
