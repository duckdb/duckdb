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

struct CompressedMaterialization {
	//! The types we compress strings to
	static const vector<LogicalType> StringCompressedTypes() {
		return {LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT, LogicalTypeId::HUGEINT};
	}

	//! String compress function name
	static const string StringCompressFunctionName(const LogicalType &result_type) {
		return StringUtil::Format("cm_compress_string_%s", LogicalTypeIdToString(result_type.id()));
	}
	//! String decompress function name
	static const string StringDecompressFunctionName() {
		return "cm_decompress_string";
	}
};

struct CompressedMaterializationIntegralCompressFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static ScalarFunction GetFunction(const LogicalType &input_type, const LogicalType &result_type);
};

struct CompressedMaterializationIntegralDecompressFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static ScalarFunction GetFunction(const LogicalType &input_type, const LogicalType &result_type);
};

struct CompressedMaterializationStringCompressFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CompressedMaterializationStringDecompressFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
