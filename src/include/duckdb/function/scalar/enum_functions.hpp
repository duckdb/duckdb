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

} // namespace duckdb
