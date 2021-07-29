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
	COMPRESSION_INVALID = 0,
	COMPRESSION_UNCOMPRESSED = 1,
	COMPRESSION_RLE = 2,
	COMPRESSION_DICTIONARY = 3,
	COMPRESSION_PFOR_DELTA = 4,
	COMPRESSION_BITPACKING = 5,
	COMPRESSION_FSST = 6
};

} // namespace duckdb
