#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/common/string.hpp"
#include <stdint.h>
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/uint128_arithmetic.hpp"

namespace duckdb {

// Forward declaration to allow conversion between hugeint and uhugeint
struct uhugeint_t; // NOLINT: use numeric casing

struct hugeint_t { // NOLINT: use numeric casing
public:
	uint64_t lower;
	int64_t upper;

public:
	hugeint_t() = default;
	//! constexpr so that arithmetic against a constant operand can be folded at compile
	//! time; the body is at the bottom of this header
	constexpr hugeint_t(int64_t value); // NOLINT: Allow implicit conversion from `int64_t`
	constexpr hugeint_t(int64_t upper, uint64_t lower) : lower(lower), upper(upper) {
	}
	constexpr hugeint_t(const hugeint_t &rhs) = default;
	constexpr hugeint_t(hugeint_t &&rhs) = default;
	hugeint_t &operator=(const hugeint_t &rhs) = default;
	hugeint_t &operator=(hugeint_t &&rhs) = default;

	DUCKDB_API string ToString() const;

	// comparison operators
	//! constexpr so that comparisons fold at compile time
	constexpr bool operator==(const hugeint_t &rhs) const;
	constexpr bool operator!=(const hugeint_t &rhs) const;
	constexpr bool operator<=(const hugeint_t &rhs) const;
	constexpr bool operator<(const hugeint_t &rhs) const;
	constexpr bool operator>(const hugeint_t &rhs) const;
	constexpr bool operator>=(const hugeint_t &rhs) const;

	// arithmetic operators
	//! constexpr so the compiler can fold these when an operand is a compile-time constant
	constexpr hugeint_t operator+(const hugeint_t &rhs) const;
	constexpr hugeint_t operator-(const hugeint_t &rhs) const;
	constexpr hugeint_t operator*(const hugeint_t &rhs) const;
	constexpr hugeint_t operator/(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator%(const hugeint_t &rhs) const;
	constexpr hugeint_t operator-() const;

	// bitwise operators
	DUCKDB_API hugeint_t operator>>(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator<<(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator&(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator|(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator^(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator~() const;

	// in-place operators
	DUCKDB_API hugeint_t &operator+=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator-=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator*=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator/=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator%=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator>>=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator<<=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator&=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator|=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator^=(const hugeint_t &rhs);

	// boolean operators
	DUCKDB_API explicit operator bool() const;
	DUCKDB_API bool operator!() const;

	// cast operators -- doesn't check bounds/overflow/underflow
	DUCKDB_API explicit operator uint8_t() const;
	DUCKDB_API explicit operator uint16_t() const;
	DUCKDB_API explicit operator uint32_t() const;
	DUCKDB_API explicit operator uint64_t() const;
	DUCKDB_API explicit operator int8_t() const;
	DUCKDB_API explicit operator int16_t() const;
	DUCKDB_API explicit operator int32_t() const;
	DUCKDB_API explicit operator int64_t() const;
	DUCKDB_API explicit operator uhugeint_t() const;
};

//===--------------------------------------------------------------------===//
// constexpr 128-bit arithmetic
//
// These live here rather than next to the rest of Hugeint because
// duckdb/common/types/hugeint.hpp includes duckdb/common/limits.hpp, which
// includes this file back. Everything the digit-parsing loops need has to be
// reachable from here, and the bodies are at the bottom of the file because a
// member function body defined inside the class is looked up at the class's
// closing brace -- above the helpers below.
//===--------------------------------------------------------------------===//

//! NumericLimits<hugeint_t>::Minimum()/Maximum(), spelled out because limits.hpp
//! includes this file back so the traits cannot be used here
constexpr hugeint_t HugeintMinimum() {
	return hugeint_t(INT64_MIN, 0);
}

constexpr hugeint_t HugeintMaximum() {
	return hugeint_t(INT64_MAX, UINT64_MAX);
}

constexpr bool HugeintEquals(hugeint_t lhs, hugeint_t rhs) {
	return lhs.upper == rhs.upper && lhs.lower == rhs.lower;
}

constexpr bool HugeintNotEquals(hugeint_t lhs, hugeint_t rhs) {
	return !HugeintEquals(lhs, rhs);
}

constexpr bool HugeintGreaterThan(hugeint_t lhs, hugeint_t rhs) {
	return lhs.upper > rhs.upper || (lhs.upper == rhs.upper && lhs.lower > rhs.lower);
}

constexpr bool HugeintGreaterThanEquals(hugeint_t lhs, hugeint_t rhs) {
	return lhs.upper > rhs.upper || (lhs.upper == rhs.upper && lhs.lower >= rhs.lower);
}

constexpr bool HugeintLessThan(hugeint_t lhs, hugeint_t rhs) {
	return lhs.upper < rhs.upper || (lhs.upper == rhs.upper && lhs.lower < rhs.lower);
}

constexpr bool HugeintLessThanEquals(hugeint_t lhs, hugeint_t rhs) {
	return lhs.upper < rhs.upper || (lhs.upper == rhs.upper && lhs.lower <= rhs.lower);
}

//! Two's-complement negation, written out because the operators below cannot call
//! operator-() on themselves without the fold failing
constexpr hugeint_t HugeintNegate(hugeint_t n) {
	return hugeint_t(static_cast<int64_t>(~static_cast<uint64_t>(n.upper) + (n.lower == 0)), 0 - n.lower);
}

constexpr hugeint_t HugeintMultiplyUnchecked(hugeint_t lhs, hugeint_t rhs) {
	// Unchecked multiplication uses the low 128 bits, including for negative operands.
#if ((__GNUC__ >= 5) || defined(__clang__)) && defined(__SIZEOF_INT128__)
	const auto left = (static_cast<__uint128_t>(static_cast<uint64_t>(lhs.upper)) << 64) | lhs.lower;
	const auto right = (static_cast<__uint128_t>(static_cast<uint64_t>(rhs.upper)) << 64) | rhs.lower;
	const auto result = left * right;
	return hugeint_t(static_cast<int64_t>(result >> 64), static_cast<uint64_t>(result));
#else
	// Multiply code adapted from:
	// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

	// split values into 4 32-bit parts
	uint64_t top[4] {uint64_t(lhs.upper) >> 32, uint64_t(lhs.upper) & 0xffffffff, lhs.lower >> 32,
	                 lhs.lower & 0xffffffff};
	uint64_t bottom[4] {uint64_t(rhs.upper) >> 32, uint64_t(rhs.upper) & 0xffffffff, rhs.lower >> 32,
	                    rhs.lower & 0xffffffff};
	uint64_t products[4][4] {};

	// multiply each component of the values
	for (idx_t x = 0; x < 4; x++) {
		for (idx_t y = 0; y < 4; y++) {
			products[x][y] = top[x] * bottom[y];
		}
	}

	// first row
	uint64_t fourth32 = (products[3][3] & 0xffffffff);
	uint64_t third32 = (products[3][2] & 0xffffffff) + (products[3][3] >> 32);
	uint64_t second32 = (products[3][1] & 0xffffffff) + (products[3][2] >> 32);
	uint64_t first32 = (products[3][0] & 0xffffffff) + (products[3][1] >> 32);

	// second row
	third32 += (products[2][3] & 0xffffffff);
	second32 += (products[2][2] & 0xffffffff) + (products[2][3] >> 32);
	first32 += (products[2][1] & 0xffffffff) + (products[2][2] >> 32);

	// third row
	second32 += (products[1][3] & 0xffffffff);
	first32 += (products[1][2] & 0xffffffff) + (products[1][3] >> 32);

	// fourth row
	first32 += (products[0][3] & 0xffffffff);

	// move carry to next digit
	third32 += fourth32 >> 32;
	second32 += third32 >> 32;
	first32 += second32 >> 32;

	// remove carry from current digit
	fourth32 &= 0xffffffff;
	third32 &= 0xffffffff;
	second32 &= 0xffffffff;
	first32 &= 0xffffffff;

	return hugeint_t(static_cast<int64_t>((first32 << 32) + second32), (third32 << 32) + fourth32);
#endif
}

//! Result of a 128-bit division. Hugeint::DivMod's out-parameter form is declared in
//! types/hugeint.hpp, which is not visible from this header
struct HugeintDivModResult {
	hugeint_t quotient;
	hugeint_t remainder;
};

//! Signed 128-bit division truncating toward zero. Dividing by zero yields a zero quotient with lhs as the
//! remainder, and MINIMUM / -1 wraps to MINIMUM.
constexpr HugeintDivModResult HugeintDivMod(hugeint_t lhs, hugeint_t rhs) {
	if (HugeintEquals(rhs, hugeint_t(0))) {
		return HugeintDivModResult {hugeint_t(0), lhs};
	}
	const bool lhs_negative = lhs.upper < 0;
	const bool rhs_negative = rhs.upper < 0;
	// MINIMUM negates to itself, and its bits read as unsigned are its magnitude 2^127
	const hugeint_t lhs_abs = lhs_negative ? HugeintNegate(lhs) : lhs;
	const hugeint_t rhs_abs = rhs_negative ? HugeintNegate(rhs) : rhs;
	const auto result = Uint128DivMod(static_cast<uint64_t>(lhs_abs.upper), lhs_abs.lower,
	                                  static_cast<uint64_t>(rhs_abs.upper), rhs_abs.lower);
	hugeint_t quotient(static_cast<int64_t>(result.quotient_upper), result.quotient_lower);
	hugeint_t remainder(static_cast<int64_t>(result.remainder_upper), result.remainder_lower);
	if (lhs_negative != rhs_negative) {
		quotient = HugeintNegate(quotient);
	}
	if (lhs_negative) {
		remainder = HugeintNegate(remainder);
	}
	return HugeintDivModResult {quotient, remainder};
}

constexpr hugeint_t::hugeint_t(int64_t value) // NOLINT: Allow implicit conversion from `int64_t`
    : lower(static_cast<uint64_t>(value)), upper(value < 0 ? -1 : 0) {
}

constexpr bool hugeint_t::operator==(const hugeint_t &rhs) const {
	return HugeintEquals(*this, rhs);
}

constexpr bool hugeint_t::operator!=(const hugeint_t &rhs) const {
	return HugeintNotEquals(*this, rhs);
}

constexpr bool hugeint_t::operator<(const hugeint_t &rhs) const {
	return HugeintLessThan(*this, rhs);
}

constexpr bool hugeint_t::operator<=(const hugeint_t &rhs) const {
	return HugeintLessThanEquals(*this, rhs);
}

constexpr bool hugeint_t::operator>(const hugeint_t &rhs) const {
	return HugeintGreaterThan(*this, rhs);
}

constexpr bool hugeint_t::operator>=(const hugeint_t &rhs) const {
	return HugeintGreaterThanEquals(*this, rhs);
}

constexpr hugeint_t hugeint_t::operator+(const hugeint_t &rhs) const {
	return hugeint_t(static_cast<int64_t>(static_cast<uint64_t>(upper) + static_cast<uint64_t>(rhs.upper) +
	                                      ((lower + rhs.lower) < lower)),
	                 lower + rhs.lower);
}

constexpr hugeint_t hugeint_t::operator-(const hugeint_t &rhs) const {
	return hugeint_t(static_cast<int64_t>(static_cast<uint64_t>(upper) - static_cast<uint64_t>(rhs.upper) -
	                                      ((lower - rhs.lower) > lower)),
	                 lower - rhs.lower);
}

constexpr hugeint_t hugeint_t::operator*(const hugeint_t &rhs) const {
	return HugeintMultiplyUnchecked(*this, rhs);
}

constexpr hugeint_t hugeint_t::operator/(const hugeint_t &rhs) const {
	// this is the unchecked division: dividing by zero yields 0 and MINIMUM / -1 wraps,
	// matching Hugeint::Divide<false>. Callers that need overflow checks use TryDivMod.
	return HugeintDivMod(*this, rhs).quotient;
}

constexpr hugeint_t hugeint_t::operator-() const {
	return HugeintNegate(*this);
}

} // namespace duckdb

namespace std {
template <>
struct hash<duckdb::hugeint_t> {
	size_t operator()(const duckdb::hugeint_t &val) const {
		using std::hash;
		return hash<int64_t> {}(val.upper) ^ hash<uint64_t> {}(val.lower);
	}
};
} // namespace std
