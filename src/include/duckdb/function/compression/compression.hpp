//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/compression/compression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

struct ConstantFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(PhysicalType type);
};

struct UncompressedFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(PhysicalType type);
};

struct RLEFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(PhysicalType type);
};

struct BitpackingFun{
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(PhysicalType type);
};

} // namespace duckdb
