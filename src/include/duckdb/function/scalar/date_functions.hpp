//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/date_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct AgeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DatePartFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DateTruncFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CurrentTimeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CurrentDateFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CurrentTimestampFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct EpochFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
