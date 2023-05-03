//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/distributive_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct CountStarFun {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct CountFun {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct FirstFun {
	static AggregateFunction GetFunction(const LogicalType &type);

	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
