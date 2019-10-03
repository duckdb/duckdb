//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar/math_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"
#include "function/function_set.hpp"

namespace duckdb {

struct Abs {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Sign {
	static void RegisterFunction(BuiltinFunctions &set);
};


struct Ceil {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Floor {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Round {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Degrees {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Radians {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Random {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Setseed {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Cbrt {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Exp {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Log2 {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Log10 {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Ln {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Pow {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct Sqrt {
	static void RegisterFunction(BuiltinFunctions &set);
};


struct Pi {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb

