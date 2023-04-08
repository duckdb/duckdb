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

struct CompressedMaterializationTypes {
	//! The types we compress integral types to
	static const vector<LogicalType> Integral() {
		return {LogicalType::UTINYINT, LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT};
	}
	//! The types we compress strings to
	static const vector<LogicalType> String() {
		return {LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT, LogicalTypeId::HUGEINT};
	}
};

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
