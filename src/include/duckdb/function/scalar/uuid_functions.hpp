//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/uuid_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct UUIDFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
