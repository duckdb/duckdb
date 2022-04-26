//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/nested_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct ListFun {
	static void RegisterFunction(BuiltinFunctions &set);
};
struct HistogramFun {
	static void RegisterFunction(BuiltinFunctions &set);
};
} // namespace duckdb
