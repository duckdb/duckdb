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

struct Base64Fun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct EncodeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
