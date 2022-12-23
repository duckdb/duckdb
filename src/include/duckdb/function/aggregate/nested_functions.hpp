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
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct HistogramFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static AggregateFunction GetHistogramUnorderedMap(LogicalType &type);
};

struct ListFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
