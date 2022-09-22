//===----------------------------------------------------------------------===//
//                         DuckDB
//
// third_party/chimp/include/chimp_utils.hpp
//
//
//===----------------------------------------------------------------------===//

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
static inline int __builtin_clzll(unsigned long long mask) {
  unsigned long where;
// BitScanReverse scans from MSB to LSB for first set bit.
// Returns 0 if no set bit is found.
#if defined(_WIN64)
  if (_BitScanReverse64(&where, mask))
    return static_cast<int>(63 - where);
#elif defined(_WIN32)
  // Scan the high 32 bits.
  if (_BitScanReverse(&where, static_cast<unsigned long>(mask >> 32)))
    return static_cast<int>(63 -
                            (where + 32)); // Create a bit offset from the MSB.
  // Scan the low 32 bits.
  if (_BitScanReverse(&where, static_cast<unsigned long>(mask)))
    return static_cast<int>(63 - where);
#else
#error "Implementation of __builtin_clzll required"
#endif
  return 64; // Undefined Behavior.
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
