//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/blob_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct GetBitFun {
	static void RegisterFunction(BuiltinFunctions &set); // step 2
};

} // namespace duckdb
