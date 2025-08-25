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
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct UncompressedFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct RLEFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct BitpackingFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct DictionaryCompressionFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct DictFSSTCompressionFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct ChimpCompressionFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct PatasCompressionFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct AlpCompressionFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct AlpRDCompressionFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct FSSTFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct ZSTDFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(PhysicalType type);
};

struct RoaringCompressionFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct EmptyValidityCompressionFun {
	static CompressionFunction GetFunction(QueryContext context, PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

} // namespace duckdb
