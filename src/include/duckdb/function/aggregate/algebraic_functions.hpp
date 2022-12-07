//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/algebraic_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct AvgFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CovarSampFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CovarPopFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Corr {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StdDevSampFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StdDevPopFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct VarPopFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct VarSampFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct VarianceFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StandardErrorOfTheMeanFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
