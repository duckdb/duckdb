//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/compression_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class CompressionType : uint8_t {
	COMPRESSION_AUTO = 0,
	COMPRESSION_UNCOMPRESSED = 1,
	COMPRESSION_CONSTANT = 2,
	COMPRESSION_RLE = 3,
	COMPRESSION_DICTIONARY = 4,
	COMPRESSION_PFOR_DELTA = 5,
	COMPRESSION_BITPACKING = 6,
	COMPRESSION_FSST = 7,
	COMPRESSION_CHIMP = 8,
	COMPRESSION_PATAS = 9
};

static CompressionType compression_options[] = {
    CompressionType::COMPRESSION_AUTO,       CompressionType::COMPRESSION_UNCOMPRESSED,
    CompressionType::COMPRESSION_CONSTANT,   CompressionType::COMPRESSION_RLE,
    CompressionType::COMPRESSION_DICTIONARY, CompressionType::COMPRESSION_PFOR_DELTA,
    CompressionType::COMPRESSION_BITPACKING, CompressionType::COMPRESSION_FSST,
    CompressionType::COMPRESSION_CHIMP,      CompressionType::COMPRESSION_PATAS};

vector<string> ListCompressionTypes();
CompressionType CompressionTypeFromString(const string &str);
string CompressionTypeToString(CompressionType type);

} // namespace duckdb
