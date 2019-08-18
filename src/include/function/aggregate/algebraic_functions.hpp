//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate/algebraic_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function.hpp"

namespace duckdb {

struct Avg {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CovarSamp {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CovarPop {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StdDevSamp {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StdDevPop {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct VarPop {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct VarSamp {
	static void RegisterFunction(BuiltinFunctions &set);
};

}
