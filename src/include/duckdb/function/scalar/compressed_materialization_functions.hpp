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

struct CMIntegralCompressFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static ScalarFunction GetFunction(const LogicalType &input_type, const LogicalType &result_type);
};

struct CMIntegralDecompressFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static ScalarFunction GetFunction(const LogicalType &input_type, const LogicalType &result_type);
};

struct CMStringCompressFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static ScalarFunction GetFunction(const LogicalType &result_type);
};

struct CMStringDecompressFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static ScalarFunction GetFunction(const LogicalType &input_type);
};

} // namespace duckdb
