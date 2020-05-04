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

namespace duckdb {

struct BitAndFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitOrFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitXorFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CountStarFun {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct CountFun {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct FirstFun {
	static AggregateFunction GetFunction(SQLType type);

	static void RegisterFunction(BuiltinFunctions &set);
};

struct MaxFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MinFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SumFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StringAggFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
