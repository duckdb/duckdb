//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bit_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/hugeint.hpp"

#ifdef _MSC_VER
#define __restrict__
#define __BYTE_ORDER__          __ORDER_LITTLE_ENDIAN__
#define __ORDER_LITTLE_ENDIAN__ 2
#include <intrin.h>
static inline int __builtin_ctzll(unsigned long long x) {
#ifdef _WIN64
	unsigned long ret;
	_BitScanForward64(&ret, x);
	return (int)ret;
#else
	unsigned long low, high;
	bool low_set = _BitScanForward(&low, (unsigned __int32)(x)) != 0;
	_BitScanForward(&high, (unsigned __int32)(x >> 32));
	high += 32;
	return low_set ? low : high;
#endif
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
		return static_cast<int>(63 - (where + 32)); // Create a bit offset from the MSB.
	// Scan the low 32 bits.
	if (_BitScanReverse(&where, static_cast<unsigned long>(mask)))
		return static_cast<int>(63 - where);
#else
#error "Implementation of __builtin_clzll required"
#endif
	return 64; // Undefined Behavior.
}

static inline int __builtin_ctz(unsigned int value) {
	unsigned long trailing_zero = 0;

	if (_BitScanForward(&trailing_zero, value)) {
		return trailing_zero;
	} else {
		// This is undefined, I better choose 32 than 0
		return 32;
	}
}

static inline int __builtin_clz(unsigned int value) {
	unsigned long leading_zero = 0;

	if (_BitScanReverse(&leading_zero, value)) {
		return 31 - leading_zero;
	} else {
		// Same remarks as above
		return 32;
	}
}

#endif

namespace duckdb {

template <class T>
struct CountZeros {};

template <>
struct CountZeros<uint32_t> {
	inline static int Leading(uint32_t value) {
		if (!value) {
			return 32;
		}
		return __builtin_clz(value);
	}
	inline static int Trailing(uint32_t value) {
		if (!value) {
			return 32;
		}
		return __builtin_ctz(value);
	}
};

template <>
struct CountZeros<uint64_t> {
	inline static int Leading(uint64_t value) {
		if (!value) {
			return 64;
		}
		return __builtin_clzll(value);
	}
	inline static int Trailing(uint64_t value) {
		if (!value) {
			return 64;
		}
		return __builtin_ctzll(value);
	}
};

template <>
struct CountZeros<hugeint_t> {
	inline static int Leading(hugeint_t value) {
		const uint64_t upper = (uint64_t)value.upper;
		const uint64_t lower = value.lower;

		if (upper) {
			return __builtin_clzll(upper);
		} else if (lower) {
			return 64 + __builtin_clzll(lower);
		} else {
			return 128;
		}
	}

	inline static int Trailing(hugeint_t value) {
		const uint64_t upper = (uint64_t)value.upper;
		const uint64_t lower = value.lower;

		if (lower) {
			return __builtin_ctzll(lower);
		} else if (upper) {
			return 64 + __builtin_ctzll(upper);
		} else {
			return 128;
		}
	}
};

} // namespace duckdb
