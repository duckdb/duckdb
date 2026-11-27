//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/uint128_arithmetic.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>

namespace duckdb {

//! Result of an unsigned 128-bit division, split into 64-bit halves
struct Uint128DivModResult {
	uint64_t quotient_upper;
	uint64_t quotient_lower;
	uint64_t remainder_upper;
	uint64_t remainder_lower;
};

//! The number of leading zero bits of a non-zero value
constexpr uint32_t Uint64LeadingZeros(uint64_t value) {
#if defined(__GNUC__) || defined(__clang__)
	return static_cast<uint32_t>(__builtin_clzll(value));
#else
	uint32_t count = 0;
	for (uint32_t step = 32; step > 0; step >>= 1) {
		if (!(value >> (64 - step))) {
			value <<= step;
			count += step;
		}
	}
	return count;
#endif
}

//! Returns the low 64 bits of a * b and sets high to its high 64 bits
constexpr uint64_t Uint64MultiplyFull(uint64_t a, uint64_t b, uint64_t &high) {
#if ((__GNUC__ >= 5) || defined(__clang__)) && defined(__SIZEOF_INT128__)
	const auto product = static_cast<__uint128_t>(a) * b;
	high = static_cast<uint64_t>(product >> 64);
	return static_cast<uint64_t>(product);
#else
	constexpr uint64_t HALF_MASK = 0xFFFFFFFFULL;
	const uint64_t low_low = (a & HALF_MASK) * (b & HALF_MASK);
	const uint64_t low_high = (a & HALF_MASK) * (b >> 32);
	const uint64_t high_low = (a >> 32) * (b & HALF_MASK);
	const uint64_t high_high = (a >> 32) * (b >> 32);
	const uint64_t middle = (low_low >> 32) + (low_high & HALF_MASK) + (high_low & HALF_MASK);
	high = high_high + (low_high >> 32) + (high_low >> 32) + (middle >> 32);
	return (low_low & HALF_MASK) | (middle << 32);
#endif
}

//! Divides high * 2^64 + low by a divisor larger than high, so that the quotient fits 64 bits; sets the remainder.
//! Two base 2^32 steps (Hacker's Delight divlu).
// no D_ASSERT: must stay usable in constant expressions
constexpr uint64_t Uint128DivRem64Portable(uint64_t high, uint64_t low, uint64_t divisor, uint64_t &remainder) {
	constexpr uint64_t HALF_BASE = static_cast<uint64_t>(1) << 32;
	constexpr uint64_t HALF_MASK = HALF_BASE - 1;
	// normalize so that the divisor has its top bit set; the quotient is unchanged, the remainder is scaled
	const uint32_t shift = Uint64LeadingZeros(divisor);
	if (shift > 0) {
		divisor <<= shift;
		high = (high << shift) | (low >> (64 - shift));
		low <<= shift;
	}
	const uint64_t divisor_high = divisor >> 32;
	const uint64_t divisor_low = divisor & HALF_MASK;
	const uint64_t low_high = low >> 32;
	const uint64_t low_low = low & HALF_MASK;
	// the high half of the quotient: an estimate from the top halves that is corrected down until it is exact
	uint64_t q_high = high / divisor_high;
	uint64_t rhat = high - q_high * divisor_high;
	while (q_high >= HALF_BASE || q_high * divisor_low > ((rhat << 32) | low_high)) {
		q_high--;
		rhat += divisor_high;
		if (rhat >= HALF_BASE) {
			break;
		}
	}
	// the partial remainder is below the divisor, so the wrapping arithmetic yields the exact value
	const uint64_t middle = (high << 32) + low_high - q_high * divisor;
	uint64_t q_low = middle / divisor_high;
	rhat = middle - q_low * divisor_high;
	while (q_low >= HALF_BASE || q_low * divisor_low > ((rhat << 32) | low_low)) {
		q_low--;
		rhat += divisor_high;
		if (rhat >= HALF_BASE) {
			break;
		}
	}
	remainder = ((middle << 32) + low_low - q_low * divisor) >> shift;
	return (q_high << 32) | q_low;
}

//! Divides high * 2^64 + low by a divisor larger than high, so that the quotient fits 64 bits; sets the remainder
constexpr uint64_t Uint128DivRem64(uint64_t high, uint64_t low, uint64_t divisor, uint64_t &remainder) {
	// clang-cl has the 128-bit type but links no runtime support for dividing it
#if ((__GNUC__ >= 5) || defined(__clang__)) && defined(__SIZEOF_INT128__) && !defined(_MSC_VER)
	const auto quotient = static_cast<uint64_t>(((static_cast<__uint128_t>(high) << 64) | low) / divisor);
	remainder = low - quotient * divisor;
	return quotient;
#else
	return Uint128DivRem64Portable(high, low, divisor, remainder);
#endif
}

//! Unsigned 128-bit division by a divisor of at least 2^64, so that the quotient fits 64 bits (Hacker's Delight
//! divDouble): the quotient is estimated from the normalized top bits of the divisor and corrected by at most one.
constexpr Uint128DivModResult Uint128DivModLargeDivisor(uint64_t lhs_upper, uint64_t lhs_lower, uint64_t rhs_upper,
                                                        uint64_t rhs_lower) {
	if (lhs_upper < rhs_upper) {
		return Uint128DivModResult {0, 0, lhs_upper, lhs_lower};
	}
	const uint32_t shift = Uint64LeadingZeros(rhs_upper);
	const uint64_t divisor_top = shift == 0 ? rhs_upper : (rhs_upper << shift) | (rhs_lower >> (64 - shift));
	// halving keeps the high half of the dividend below the normalized divisor, as the 128-by-64-bit step requires
	uint64_t unused_remainder = 0;
	const uint64_t estimate =
	    Uint128DivRem64(lhs_upper >> 1, (lhs_upper << 63) | (lhs_lower >> 1), divisor_top, unused_remainder);
	uint64_t quotient = estimate >> (63 - shift);
	if (quotient != 0) {
		quotient--;
	}
	// the quotient does not exceed the exact one, so the product fits 128 bits and the remainder is non-negative
	uint64_t product_upper = 0;
	const uint64_t product_lower = Uint64MultiplyFull(quotient, rhs_lower, product_upper);
	product_upper += quotient * rhs_upper;
	uint64_t remainder_lower = lhs_lower - product_lower;
	uint64_t remainder_upper = lhs_upper - product_upper - (lhs_lower < product_lower ? 1 : 0);
	if (remainder_upper > rhs_upper || (remainder_upper == rhs_upper && remainder_lower >= rhs_lower)) {
		const uint64_t borrow = remainder_lower < rhs_lower ? 1 : 0;
		remainder_lower -= rhs_lower;
		remainder_upper -= rhs_upper + borrow;
		quotient++;
	}
	return Uint128DivModResult {0, quotient, remainder_upper, remainder_lower};
}

//! Unsigned division of a 128-bit dividend by a non-zero 64-bit divisor
constexpr Uint128DivModResult Uint128DivModSmallDivisor(uint64_t lhs_upper, uint64_t lhs_lower, uint64_t divisor) {
	if (lhs_upper == 0) {
		return Uint128DivModResult {0, lhs_lower / divisor, 0, lhs_lower % divisor};
	}
	// the high step leaves a remainder below the divisor, as the low step requires
	const uint64_t quotient_upper = lhs_upper / divisor;
	uint64_t remainder = 0;
	const uint64_t quotient_lower = Uint128DivRem64(lhs_upper % divisor, lhs_lower, divisor, remainder);
	return Uint128DivModResult {quotient_upper, quotient_lower, 0, remainder};
}

//! Unsigned 128-bit division. Precondition: the divisor is not zero.
constexpr Uint128DivModResult Uint128DivMod(uint64_t lhs_upper, uint64_t lhs_lower, uint64_t rhs_upper,
                                            uint64_t rhs_lower) {
	if (rhs_upper != 0) {
		return Uint128DivModLargeDivisor(lhs_upper, lhs_lower, rhs_upper, rhs_lower);
	}
	return Uint128DivModSmallDivisor(lhs_upper, lhs_lower, rhs_lower);
}

} // namespace duckdb
