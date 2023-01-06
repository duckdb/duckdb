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
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct AgeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DateDiffFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DatePartFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DateSubFun {
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

struct MakeDateFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StrfTimeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StrpTimeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ToIntervalFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
