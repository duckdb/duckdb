//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/hugeint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

//! The Hugeint class contains static operations for the INT128 type
class Hugeint {
public:
	//! Convert a string to a hugeint object
	static bool FromString(string str, hugeint_t &result);
	//! Convert a string to a hugeint object
	static bool FromCString(const char *str, idx_t len, hugeint_t &result);
	//! Convert a hugeint object to a string
	static string ToString(hugeint_t input);

	static hugeint_t FromString(string str) {
		hugeint_t result;
		FromString(str, result);
		return result;
	}

	template <class T>
	static bool TryCast(hugeint_t input, T &result);

	template <class T>
	static T Cast(hugeint_t input) {
		T value;
		TryCast(input, value);
		return value;
	}

	template <class T>
	static bool TryConvert(T value, hugeint_t &result);

	template <class T>
	static hugeint_t Convert(T value) {
		hugeint_t result;
		if (!TryConvert(value, result)) { // LCOV_EXCL_START
			throw ValueOutOfRangeException(double(value), GetTypeId<T>(), GetTypeId<hugeint_t>());
		} // LCOV_EXCL_STOP
		return result;
	}

	static void NegateInPlace(hugeint_t &input) {
		if (input.upper == NumericLimits<int64_t>::Minimum() && input.lower == 0) {
			throw OutOfRangeException("HUGEINT is out of range");
		}
		input.lower = NumericLimits<uint64_t>::Maximum() - input.lower + 1;
		input.upper = -1 - input.upper + (input.lower == 0);
	}
	static hugeint_t Negate(hugeint_t input) {
		NegateInPlace(input);
		return input;
	}

	static bool TryMultiply(hugeint_t lhs, hugeint_t rhs, hugeint_t &result);

	static hugeint_t Add(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Subtract(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Multiply(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Divide(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Modulo(hugeint_t lhs, hugeint_t rhs);

	// DivMod -> returns the result of the division (lhs / rhs), and fills up the remainder
	static hugeint_t DivMod(hugeint_t lhs, hugeint_t rhs, hugeint_t &remainder);
	// DivMod but lhs MUST be positive, and rhs is a uint64_t
	static hugeint_t DivModPositive(hugeint_t lhs, uint64_t rhs, uint64_t &remainder);

	static bool AddInPlace(hugeint_t &lhs, hugeint_t rhs);
	static bool SubtractInPlace(hugeint_t &lhs, hugeint_t rhs);

	// comparison operators
	// note that everywhere here we intentionally use bitwise ops
	// this is because they seem to be consistently much faster (benchmarked on a Macbook Pro)
	static bool Equals(hugeint_t lhs, hugeint_t rhs) {
		int lower_equals = lhs.lower == rhs.lower;
		int upper_equals = lhs.upper == rhs.upper;
		return lower_equals & upper_equals;
	}
	static bool NotEquals(hugeint_t lhs, hugeint_t rhs) {
		int lower_not_equals = lhs.lower != rhs.lower;
		int upper_not_equals = lhs.upper != rhs.upper;
		return lower_not_equals | upper_not_equals;
	}
	static bool GreaterThan(hugeint_t lhs, hugeint_t rhs) {
		int upper_bigger = lhs.upper > rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_bigger = lhs.lower > rhs.lower;
		return upper_bigger | (upper_equal & lower_bigger);
	}
	static bool GreaterThanEquals(hugeint_t lhs, hugeint_t rhs) {
		int upper_bigger = lhs.upper > rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_bigger_equals = lhs.lower >= rhs.lower;
		return upper_bigger | (upper_equal & lower_bigger_equals);
	}
	static bool LessThan(hugeint_t lhs, hugeint_t rhs) {
		int upper_smaller = lhs.upper < rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_smaller = lhs.lower < rhs.lower;
		return upper_smaller | (upper_equal & lower_smaller);
	}
	static bool LessThanEquals(hugeint_t lhs, hugeint_t rhs) {
		int upper_smaller = lhs.upper < rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_smaller_equals = lhs.lower <= rhs.lower;
		return upper_smaller | (upper_equal & lower_smaller_equals);
	}
	static const hugeint_t POWERS_OF_TEN[40];
};

template <>
bool Hugeint::TryCast(hugeint_t input, int8_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, int16_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, int32_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, int64_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, uint8_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, uint16_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, uint32_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, uint64_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, hugeint_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, float &result);
template <>
bool Hugeint::TryCast(hugeint_t input, double &result);
template <>
bool Hugeint::TryCast(hugeint_t input, long double &result);

template <>
bool Hugeint::TryConvert(int8_t value, hugeint_t &result);
template <>
bool Hugeint::TryConvert(int16_t value, hugeint_t &result);
template <>
bool Hugeint::TryConvert(int32_t value, hugeint_t &result);
template <>
bool Hugeint::TryConvert(int64_t value, hugeint_t &result);
template <>
bool Hugeint::TryConvert(uint8_t value, hugeint_t &result);
template <>
bool Hugeint::TryConvert(uint16_t value, hugeint_t &result);
template <>
bool Hugeint::TryConvert(uint32_t value, hugeint_t &result);
template <>
bool Hugeint::TryConvert(uint64_t value, hugeint_t &result);
template <>
bool Hugeint::TryConvert(float value, hugeint_t &result);
template <>
bool Hugeint::TryConvert(double value, hugeint_t &result);
template <>
bool Hugeint::TryConvert(long double value, hugeint_t &result);

} // namespace duckdb
