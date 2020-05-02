//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/math_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct AbsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SignFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CeilFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct FloorFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RoundFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DegreesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RadiansFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RandomFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SetseedFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CbrtFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ExpFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Log2Fun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Log10Fun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LnFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PowFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SqrtFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PiFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitCountFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
