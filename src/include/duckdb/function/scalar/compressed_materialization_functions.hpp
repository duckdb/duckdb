//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/compressed_materialization_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CompressedMaterializationIntegralCompressFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static string GetFunctionName(const LogicalType &result_type);
};

struct CompressedMaterializationIntegralDecompressFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static string GetFunctionName(const LogicalType &result_type);
};

struct CompressedMaterializationStringFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
