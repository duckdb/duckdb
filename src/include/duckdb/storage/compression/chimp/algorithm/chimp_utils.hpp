//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/algorithm/chimp_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"

namespace duckdb {

template <class T>
struct SignificantBits {};

template <>
struct SignificantBits<uint64_t> {
	static constexpr uint8_t size = 6;
	static constexpr uint8_t mask = ((uint8_t)1 << size) - 1;
};

template <>
struct SignificantBits<uint32_t> {
	static constexpr uint8_t size = 5;
	static constexpr uint8_t mask = ((uint8_t)1 << size) - 1;
};

struct ChimpConstants {
	struct Compression {
		static constexpr uint8_t LEADING_ROUND[] = {0,  0,  0,  0,  0,  0,  0,  0,  8,  8,  8,  8,  12, 12, 12, 12,
		                                            16, 16, 18, 18, 20, 20, 22, 22, 24, 24, 24, 24, 24, 24, 24, 24,
		                                            24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
		                                            24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24};
		static constexpr uint8_t LEADING_REPRESENTATION[] = {
		    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7,
		    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7};
	};
	struct Decompression {
		static constexpr uint8_t LEADING_REPRESENTATION[] = {0, 8, 12, 16, 18, 20, 22, 24};
	};
	static constexpr uint8_t BUFFER_SIZE = 128;
	enum class Flags : uint8_t {
		VALUE_IDENTICAL = 0,
		TRAILING_EXCEEDS_THRESHOLD = 1,
		LEADING_ZERO_EQUALITY = 2,
		LEADING_ZERO_LOAD = 3
	};
};

} // namespace duckdb
