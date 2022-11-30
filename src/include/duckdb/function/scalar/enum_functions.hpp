//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/enum_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct EnumFirst {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct EnumLast {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct EnumCode {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct EnumRange {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct EnumRangeBoundary {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
