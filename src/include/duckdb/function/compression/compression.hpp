//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/compression/compression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/compression_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct ConstantFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const CompressionInfo &info);
};

struct UncompressedFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const CompressionInfo &info);
};

struct RLEFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const CompressionInfo &info);
};

struct BitpackingFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const CompressionInfo &info);
};

struct DictionaryCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const CompressionInfo &info);
};

struct ChimpCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const CompressionInfo &info);
};

struct PatasCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const CompressionInfo &info);
};

struct AlpCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const CompressionInfo &info);
};

struct AlpRDCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const CompressionInfo &info);
};

struct FSSTFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const CompressionInfo &info);
};

} // namespace duckdb
