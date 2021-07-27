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
	COMPRESSION_RLE = 1,
	COMPRESSION_DICTIONARY = 2,
	COMPRESSION_PFOR_DELTA = 3,
	COMPRESSION_BITPACKING = 4,
	COMPRESSION_FSST = 5
};

} // namespace duckdb
