//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar/sequence_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"
#include "function/function_set.hpp"

namespace duckdb {

struct NextvalFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
