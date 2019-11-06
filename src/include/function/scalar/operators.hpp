//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar/operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"
#include "function/function_set.hpp"

namespace duckdb {

struct AddFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SubtractFun {
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

} // namespace duckdb
