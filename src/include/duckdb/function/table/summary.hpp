//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/summary.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct SummaryTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
