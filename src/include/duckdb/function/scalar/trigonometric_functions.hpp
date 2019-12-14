//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/trigonometric_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct SinFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CosFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct TanFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct AsinFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct AcosFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct AtanFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CotFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Atan2Fun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
