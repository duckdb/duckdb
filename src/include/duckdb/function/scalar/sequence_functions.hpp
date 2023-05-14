//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/sequence_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct NextvalFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CurrvalFun {
	static void RegisterFunction(BuiltinFunctions &set);
};
} // namespace duckdb
