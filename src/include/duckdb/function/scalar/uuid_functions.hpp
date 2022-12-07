//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/uuid_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct UUIDFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
