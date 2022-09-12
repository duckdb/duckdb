#pragma once

namespace duckdb_chimp {

#ifdef _MSC_VER
#define __restrict__ 
#define __BYTE_ORDER__ __ORDER_LITTLE_ENDIAN__
#define __ORDER_LITTLE_ENDIAN__ 2
#include <intrin.h>
static inline int __builtin_ctzll(unsigned long long x) {
#  ifdef _WIN64
	unsigned long ret;
    _BitScanForward64(&ret, x);
	return (int)ret;
#  else
	unsigned long low, high;
	bool low_set = _BitScanForward(&low, (unsigned __int32)(x)) != 0;
	_BitScanForward(&high, (unsigned __int32)(x >> 32));
	high += 32;
	return low_set ? low : high;
#  endif
}
static inline int __builtin_clzll(unsigned long long x) {
#  ifdef _WIN64
	unsigned long ret;
	_BitScanReverse64(&ret, x);
	return (int)ret;
#  else
	unsigned long ret;
	// Scan the high 32 bits.
	if (_BitScanReverse(&ret, (uint32_t)(x >> 32))) {
		return (63 ^ (ret + 32));
	}
	// Scan the low 32 bits.
	_BitScanReverse(&ret, (uint32_t)x);
	return (63 ^ (int)ret);
#  endif
}
#endif

struct ChimpCompressionConstants {
	static constexpr uint8_t LEADING_REPRESENTATION[] = {
		0, 0, 0, 0, 0, 0, 0, 0,
		1, 1, 1, 1, 2, 2, 2, 2,
		3, 3, 4, 4, 5, 5, 6, 6,
		7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7
	};

	static constexpr uint8_t LEADING_ROUND[] = {
		0,  0,  0,  0,  0,  0,  0,  0,
		8,  8,  8,  8,  12, 12, 12, 12,
		16, 16, 18, 18, 20, 20, 22, 22,
		24, 24, 24, 24, 24, 24, 24, 24,
		24, 24, 24, 24, 24, 24, 24, 24,
		24, 24, 24, 24, 24, 24, 24, 24,
		24, 24, 24, 24, 24, 24, 24, 24,
		24, 24, 24, 24, 24, 24, 24, 24
	};
};

struct ChimpDecompressionConstants {
	static constexpr uint8_t LEADING_REPRESENTATION[] = {
		0, 8, 12, 16, 18, 20, 22, 24
	};
};

} //namespace duckdb_chimp
