//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/generic_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct LeastFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct GreatestFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ConstantOrNull {
	static ScalarFunction GetFunction(LogicalType return_type);
	static unique_ptr<FunctionData> Bind(Value value);
};

} // namespace duckdb
