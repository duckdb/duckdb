//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct AddFun {
	static ScalarFunction GetFunction(const LogicalType &type);
	static ScalarFunction GetFunction(const LogicalType &left_type, const LogicalType &right_type);
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SubtractFun {
	static ScalarFunction GetFunction(const LogicalType &type);
	static ScalarFunction GetFunction(const LogicalType &left_type, const LogicalType &right_type);
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MultiplyFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DivideFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ModFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LeftShiftFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RightShiftFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitwiseAndFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitwiseOrFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitwiseXorFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitwiseNotFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
