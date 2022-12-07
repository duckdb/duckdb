//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/regression_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {
struct RegrAvgxFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrAvgyFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrCountFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrSlopeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrR2Fun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrSXXFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrSYYFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrSXYFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrInterceptFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
