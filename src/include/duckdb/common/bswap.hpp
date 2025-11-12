//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bswap.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/numeric_utils.hpp"

#include <cstring>

namespace duckdb {

#ifndef DUCKDB_IS_BIG_ENDIAN
#if defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define DUCKDB_IS_BIG_ENDIAN 1
#else
#define DUCKDB_IS_BIG_ENDIAN 0
#endif
#endif

#if defined(__clang__) || defined(__GNUC__) && (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 3))
#define BSWAP16(x) __builtin_bswap16(static_cast<uint16_t>(x))
#define BSWAP32(x) __builtin_bswap32(static_cast<uint32_t>(x))
#define BSWAP64(x) __builtin_bswap64(static_cast<uint64_t>(x))
#else
#define BSWAP16(x) ((uint16_t)((((uint16_t)(x)&0xff00) >> 8) | (((uint16_t)(x)&0x00ff) << 8)))

#define BSWAP32(x)                                                                                                     \
	((uint32_t)((((uint32_t)(x)&0xff000000) >> 24) | (((uint32_t)(x)&0x00ff0000) >> 8) |                               \
	            (((uint32_t)(x)&0x0000ff00) << 8) | (((uint32_t)(x)&0x000000ff) << 24)))

#define BSWAP64(x)                                                                                                     \
	((uint64_t)((((uint64_t)(x)&0xff00000000000000ull) >> 56) | (((uint64_t)(x)&0x00ff000000000000ull) >> 40) |        \
	            (((uint64_t)(x)&0x0000ff0000000000ull) >> 24) | (((uint64_t)(x)&0x000000ff00000000ull) >> 8) |         \
	            (((uint64_t)(x)&0x00000000ff000000ull) << 8) | (((uint64_t)(x)&0x0000000000ff0000ull) << 24) |         \
	            (((uint64_t)(x)&0x000000000000ff00ull) << 40) | (((uint64_t)(x)&0x00000000000000ffull) << 56)))
#endif

static inline int8_t BSwap(const int8_t &x) {
	return x;
}

static inline uint8_t BSwap(const uint8_t &x) {
	return x;
}

static inline uint16_t BSwap(const uint16_t &x) {
	return BSWAP16(x);
}

static inline int16_t BSwap(const int16_t &x) {
	return static_cast<int16_t>(BSWAP16(x));
}

static inline uint32_t BSwap(const uint32_t &x) {
	return BSWAP32(x);
}

static inline int32_t BSwap(const int32_t &x) {
	return static_cast<int32_t>(BSWAP32(x));
}

static inline uint64_t BSwap(const uint64_t &x) {
	return BSWAP64(x);
}

static inline int64_t BSwap(const int64_t &x) {
	return static_cast<int64_t>(BSWAP64(x));
}

static inline uhugeint_t BSwap(const uhugeint_t &x) {
	return uhugeint_t(BSWAP64(x.upper), BSWAP64(x.lower));
}

static inline hugeint_t BSwap(const hugeint_t &x) {
	return hugeint_t(static_cast<int64_t>(BSWAP64(x.upper)), BSWAP64(x.lower));
}

static inline float BSwap(const float &x) {
	uint32_t temp;
	std::memcpy(&temp, &x, sizeof(temp));
	temp = BSWAP32(temp);
	float result;
	std::memcpy(&result, &temp, sizeof(result));
	return result;
}

static inline double BSwap(const double &x) {
	uint64_t temp;
	std::memcpy(&temp, &x, sizeof(temp));
	temp = BSWAP64(temp);
	double result;
	std::memcpy(&result, &temp, sizeof(result));
	return result;
}

template <class T>
static inline T BSwapIfLE(const T &x) {
#if DUCKDB_IS_BIG_ENDIAN
	return x;
#else
	return BSwap(x);
#endif
}

template <class T>
static inline T BSwapIfBE(const T &x) {
#if DUCKDB_IS_BIG_ENDIAN
	return BSwap(x);
#else
	return x;
#endif
}

} // namespace duckdb
