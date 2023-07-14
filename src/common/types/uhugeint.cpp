#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/windows_undefs.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

//===--------------------------------------------------------------------===//
// String Conversion
//===--------------------------------------------------------------------===//
const uhugeint_t Uhugeint::POWERS_OF_TEN[] {
    uhugeint_t(1),
    uhugeint_t(10),
    uhugeint_t(100),
    uhugeint_t(1000),
    uhugeint_t(10000),
    uhugeint_t(100000),
    uhugeint_t(1000000),
    uhugeint_t(10000000),
    uhugeint_t(100000000),
    uhugeint_t(1000000000),
    uhugeint_t(10000000000),
    uhugeint_t(100000000000),
    uhugeint_t(1000000000000),
    uhugeint_t(10000000000000),
    uhugeint_t(100000000000000),
    uhugeint_t(1000000000000000),
    uhugeint_t(10000000000000000),
    uhugeint_t(100000000000000000),
    uhugeint_t(1000000000000000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(10),
    uhugeint_t(1000000000000000000) * uhugeint_t(100),
    uhugeint_t(1000000000000000000) * uhugeint_t(1000),
    uhugeint_t(1000000000000000000) * uhugeint_t(10000),
    uhugeint_t(1000000000000000000) * uhugeint_t(100000),
    uhugeint_t(1000000000000000000) * uhugeint_t(1000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(10000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(100000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(1000000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(10000000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(100000000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(1000000000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(10000000000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(100000000000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(1000000000000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(10000000000000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(100000000000000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(1000000000000000000),
    uhugeint_t(1000000000000000000) * uhugeint_t(1000000000000000000) * uhugeint_t(10),
    uhugeint_t(1000000000000000000) * uhugeint_t(1000000000000000000) * uhugeint_t(100)};

uhugeint_t Uhugeint::DivMod() {

}

string Uhugeint::ToString(uhugeint_t input) {
	uint64_t remainder;
	string result;
	while (true) {
		if (!input.lower && !input.upper) {
			break;
		}
		input = Uhugeint::DivMod(input, 10, remainder);
		result = string(1, '0' + remainder) + result; // NOLINT
	}
	if (result.empty()) {
		// value is zero
		return "0";
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Multiply
//===--------------------------------------------------------------------===//
bool Uhugeint::TryMultiply(uhugeint_t lhs, uhugeint_t rhs, uhugeint_t &result) {
#if ((__GNUC__ >= 5) || defined(__clang__)) && defined(__SIZEOF_INT128__)
	__uint128_t left = __uint128_t(lhs.lower) + (__uint128_t(lhs.upper) << 64);
	__uint128_t right = __uint128_t(rhs.lower) + (__uint128_t(rhs.upper) << 64);
	__uint128_t result_u128;
	if (__builtin_mul_overflow(left, right, &result_u128)) {
		return false;
	}
	uint64_t upper = uint64_t(result_u128 >> 64);
	if (upper & 0x8000000000000000) { // overflow check?
		return false;
	}
	result.upper = uint64_t(upper);
	result.lower = uint64_t(result_u128 & 0xffffffffffffffff);
#else

#endif
}

uhugeint_t Uhugeint::Multiply(uhugeint_t lhs, uhugeint_t rhs) {
	uhugeint_t result;
	if (!TryMultiply(lhs, rhs, result)) {
		throw OutOfRangeException("Overflow in UHUGEINT multiplication!");
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Add/Subtract
//===--------------------------------------------------------------------===//
bool Uhugeint::AddInPlace(uhugeint_t &lhs, uhugeint_t rhs) {
	uint64_t new_upper = lhs.upper + rhs.upper + ((lhs.lower + rhs.lower) < lhs.lower);
	if (new_upper < lhs.upper) {
		return false;
	}
	lhs.upper = new_upper;
	lhs.lower += rhs.lower;
	return true;
}

bool Uhugeint::SubtractInPlace(uhugeint_t &lhs, uhugeint_t rhs) {
	uint64_t new_upper = lhs.upper - rhs.upper - ((lhs.lower - rhs.lower) > lhs.lower);
	if (new_upper > lhs.upper) {
		return false;
	}
	lhs.lower -= rhs.lower;
	return true;
}

uhugeint_t Uhugeint::Add(uhugeint_t lhs, uhugeint_t rhs) {
	if (!AddInPlace(lhs, rhs)) {
		throw OutOfRangeException("Overflow in UHUGEINT addition");
	}
	return lhs;
}

uhugeint_t Uhugeint::Subtract(uhugeint_t lhs, uhugeint_t rhs) {
	if (!SubtractInPlace(lhs, rhs)) {
		throw OutOfRangeException("Underflow in UHUGEINT addition");
	}
	return lhs;
}

//===--------------------------------------------------------------------===//
// uhugeint_t operators
//===--------------------------------------------------------------------===//
uhugeint_t::uhugeint_t(uint64_t value) {
	auto result = Uhugeint::Convert(value);
	this->lower = result.lower;
	this->upper = result.upper;
}

bool uhugeint_t::operator==(const uhugeint_t &rhs) const {
	return Uhugeint::Equals(*this, rhs);
}

bool uhugeint_t::operator!=(const uhugeint_t &rhs) const {
	return Uhugeint::NotEquals(*this, rhs);
}

bool uhugeint_t::operator<(const uhugeint_t &rhs) const {
	return Uhugeint::LessThan(*this, rhs);
}

bool uhugeint_t::operator<=(const uhugeint_t &rhs) const {
	return Uhugeint::LessThanEquals(*this, rhs);
}

bool uhugeint_t::operator>(const uhugeint_t &rhs) const {
	return Uhugeint::GreaterThan(*this, rhs);
}

bool uhugeint_t::operator>=(const uhugeint_t &rhs) const {
	return Uhugeint::GreaterThanEquals(*this, rhs);
}

uhugeint_t uhugeint_t::operator+(const uhugeint_t &rhs) const {
	return Uhugeint::Add(*this, rhs);
}

uhugeint_t uhugeint_t::operator-(const uhugeint_t &rhs) const {
	return Uhugeint::Subtract(*this, rhs);
}

uhugeint_t uhugeint_t::operator*(const uhugeint_t &rhs) const {
	return Uhugeint::Multiply(*this, rhs);
}

uhugeint_t uhugeint_t::operator/(const uhugeint_t &rhs) const {
	return Uhugeint::Divide(*this, rhs);
}

uhugeint_t uhugeint_t::operator%(const uhugeint_t &rhs) const {
	return Uhugeint::Modulo(*this, rhs);
}

uhugeint_t uhugeint_t::operator-() const {
	return Uhugeint::Negate(*this);
}

uhugeint_t uhugeint_t::operator>>(const uhugeint_t &rhs) const {
	const uint64_t shift = rhs.lower;
	if (rhs.upper != 0 || shift >= 128) {
		return uhugeint_t(0);
	} else if (shift == 0) {
		return *this;
	} else if (shift == 64) {
		return uhugeint_t(0, upper);
	} else if (shift < 64) {
		return uhugeint_t(upper >> shift, (upper << (64 - shift)) + (lower >> shift));
	} else if ((128 > shift) && (shift > 64)) {
		return uhugeint_t(0, (upper >> (shift - 64)));
	}
	return uhugeint_t(0);
}

uhugeint_t uhugeint_t::operator<<(const uhugeint_t &rhs) const {
	const uint64_t shift = rhs.lower;
	if (rhs.upper != 0 || shift >= 128) {
		return uhugeint_t(0);
	} else if (shift == 0) {
		return *this;
	} else if (shift == 64) {
		return uhugeint_t(lower, 0);
	} else if (shift < 64) {
		return uhugeint_t((upper << shift) + (lower >> (64 - shift)), lower << shift);
	} else if ((128 > shift) && (shift > 64)) {
		return uhugeint_t(lower << (shift - 64), 0);
	}
	return uhugeint_t(0);
}

uhugeint_t uhugeint_t::operator&(const uhugeint_t &rhs) const {
	uhugeint_t result;
	result.lower = lower & rhs.lower;
	result.upper = upper & rhs.upper;
	return result;
}

uhugeint_t uhugeint_t::operator|(const uhugeint_t &rhs) const {
	uhugeint_t result;
	result.lower = lower | rhs.lower;
	result.upper = upper | rhs.upper;
	return result;
}

uhugeint_t uhugeint_t::operator^(const uhugeint_t &rhs) const {
	uhugeint_t result;
	result.lower = lower ^ rhs.lower;
	result.upper = upper ^ rhs.upper;
	return result;
}

uhugeint_t uhugeint_t::operator~() const {
	uhugeint_t result;
	result.lower = ~lower;
	result.upper = ~upper;
	return result;
}

uhugeint_t &uhugeint_t::operator+=(const uhugeint_t &rhs) {
	Uhugeint::AddInPlace(*this, rhs);
	return *this;
}
uhugeint_t &uhugeint_t::operator-=(const uhugeint_t &rhs) {
	Uhugeint::SubtractInPlace(*this, rhs);
	return *this;
}
uhugeint_t &uhugeint_t::operator*=(const uhugeint_t &rhs) {
	*this = Uhugeint::Multiply(*this, rhs);
	return *this;
}
uhugeint_t &uhugeint_t::operator/=(const uhugeint_t &rhs) {
	*this = Uhugeint::Divide(*this, rhs);
	return *this;
}
uhugeint_t &uhugeint_t::operator%=(const uhugeint_t &rhs) {
	*this = Uhugeint::Modulo(*this, rhs);
	return *this;
}
uhugeint_t &uhugeint_t::operator>>=(const uhugeint_t &rhs) {
	*this = *this >> rhs;
	return *this;
}
uhugeint_t &uhugeint_t::operator<<=(const uhugeint_t &rhs) {
	*this = *this << rhs;
	return *this;
}
uhugeint_t &uhugeint_t::operator&=(const uhugeint_t &rhs) {
	lower &= rhs.lower;
	upper &= rhs.upper;
	return *this;
}
uhugeint_t &uhugeint_t::operator|=(const uhugeint_t &rhs) {
	lower |= rhs.lower;
	upper |= rhs.upper;
	return *this;
}
uhugeint_t &uhugeint_t::operator^=(const uhugeint_t &rhs) {
	lower ^= rhs.lower;
	upper ^= rhs.upper;
	return *this;
}

string uhugeint_t::ToString() const {
	return Uhugeint::ToString(*this);
}

} // namespace duckdb
