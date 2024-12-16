//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/compression_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

enum class CompressionType : uint8_t {
	COMPRESSION_AUTO = 0,
	COMPRESSION_UNCOMPRESSED = 1,
	COMPRESSION_CONSTANT = 2, // internal only
	COMPRESSION_RLE = 3,
	COMPRESSION_DICTIONARY = 4,
	COMPRESSION_PFOR_DELTA = 5,
	COMPRESSION_BITPACKING = 6,
	COMPRESSION_FSST = 7,
	COMPRESSION_CHIMP = 8,
	COMPRESSION_PATAS = 9,
	COMPRESSION_ALP = 10,
	COMPRESSION_ALPRD = 11,
	COMPRESSION_ZSTD = 12,
	COMPRESSION_ROARING = 13,
	COMPRESSION_EMPTY = 14, // internal only
	COMPRESSION_COUNT       // This has to stay the last entry of the type!
};

bool CompressionTypeIsDeprecated(CompressionType compression_type);
vector<string> ListCompressionTypes(void);
CompressionType CompressionTypeFromString(const string &str);
string CompressionTypeToString(CompressionType type);

} // namespace duckdb
