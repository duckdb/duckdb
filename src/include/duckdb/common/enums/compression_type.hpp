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
	COMPRESSION_CONSTANT = 2,
	COMPRESSION_RLE = 3,
	COMPRESSION_DICTIONARY = 4,
	COMPRESSION_PFOR_DELTA = 5,
	COMPRESSION_BITPACKING = 6,
	COMPRESSION_FSST = 7,
	COMPRESSION_CHIMP = 8,
	COMPRESSION_PATAS = 9,
	COMPRESSION_COUNT // This has to stay the last entry of the type!
};

vector<string> ListCompressionTypes(void);
CompressionType CompressionTypeFromString(const string &str);
string CompressionTypeToString(CompressionType type);

} // namespace duckdb
