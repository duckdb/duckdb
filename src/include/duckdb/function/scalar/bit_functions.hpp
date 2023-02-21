//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/bit_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct GetBitFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SetBitFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitPositionFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
