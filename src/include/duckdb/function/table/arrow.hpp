//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/arrow.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct ArrowTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
