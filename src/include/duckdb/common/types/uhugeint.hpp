//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/uhugeint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/type_util.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/uhugeint.hpp"

namespace duckdb {

//! The uhugeint class contains static operations for the UINT128 type
class Uhugeint {
public:
	//! Convert a uhugeint object to a string
	static string ToString(uhugeint_t input);

	template <class T>
	DUCKDB_API static bool TryCast(uhugeint_t input, T &result);

	template <class T>
	static T Cast(uhugeint_t input) {
		T result = 0;
		TryCast(input, result);
		return result;
	}

	template <class T>
	static bool TryConvert(T value, uhugeint_t &result);

	template <class T>
	static uhugeint_t Convert(T value) {
		uhugeint_t result;
		if (!TryConvert(value, result)) { // LCOV_EXCL_START
			throw ValueOutOfRangeException(double(value), GetTypeId<T>(), GetTypeId<uhugeint_t>());
		} // LCOV_EXCL_STOP
		return result;
	}

	// "The negative of an unsigned quantity is computed by subtracting its value from 2^n, where n is the number of
	// bits in the promoted operand."
	static void NegateInPlace(uhugeint_t &input) {
		uhugeint_t result = 0;
		SubtractInPlaceNoOverflowCheck(result, input);
		input = result;
	}
	static uhugeint_t Negate(uhugeint_t input) {
		NegateInPlace(input);
		return input;
	}

	static bool TryMultiply(uhugeint_t lhs, uhugeint_t rhs, uhugeint_t &result);

	static uhugeint_t Add(uhugeint_t lhs, uhugeint_t rhs);
	static uhugeint_t Subtract(uhugeint_t lhs, uhugeint_t rhs);
	static uhugeint_t Multiply(uhugeint_t lhs, uhugeint_t rhs);
	static uhugeint_t Divide(uhugeint_t lhs, uhugeint_t rhs);
	static uhugeint_t Modulo(uhugeint_t lhs, uhugeint_t rhs);

	// DivMod -> returns the result of the division (lhs / rhs), and fills up the remainder
	static uhugeint_t DivMod(uhugeint_t lhs, uhugeint_t rhs, uhugeint_t &remainder);

	static bool AddInPlace(uhugeint_t &lhs, uhugeint_t rhs);
	static bool SubtractInPlace(uhugeint_t &lhs, uhugeint_t rhs);

	static void SubtractInPlaceNoOverflowCheck(uhugeint_t &lhs, uhugeint_t rhs);

	// comparison operators
	// note that everywhere here we intentionally use bitwise ops
	// this is because they seem to be consistently much faster (benchmarked on a Macbook Pro)
	static bool Equals(uhugeint_t lhs, uhugeint_t rhs) {
		int lower_equals = lhs.lower == rhs.lower;
		int upper_equals = lhs.upper == rhs.upper;
		return lower_equals & upper_equals;
	}
	static bool NotEquals(uhugeint_t lhs, uhugeint_t rhs) {
		int lower_not_equals = lhs.lower != rhs.lower;
		int upper_not_equals = lhs.upper != rhs.upper;
		return lower_not_equals | upper_not_equals;
	}
	static bool GreaterThan(uhugeint_t lhs, uhugeint_t rhs) {
		int upper_bigger = lhs.upper > rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_bigger = lhs.lower > rhs.lower;
		return upper_bigger | (upper_equal & lower_bigger);
	}
	static bool GreaterThanEquals(uhugeint_t lhs, uhugeint_t rhs) {
		int upper_bigger = lhs.upper > rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_bigger_equals = lhs.lower >= rhs.lower;
		return upper_bigger | (upper_equal & lower_bigger_equals);
	}
	static bool LessThan(uhugeint_t lhs, uhugeint_t rhs) {
		int upper_smaller = lhs.upper < rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_smaller = lhs.lower < rhs.lower;
		return upper_smaller | (upper_equal & lower_smaller);
	}
	static bool LessThanEquals(uhugeint_t lhs, uhugeint_t rhs) {
		int upper_smaller = lhs.upper < rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_smaller_equals = lhs.lower <= rhs.lower;
		return upper_smaller | (upper_equal & lower_smaller_equals);
	}
	static const uhugeint_t POWERS_OF_TEN[40];
};

template <>
DUCKDB_API bool Uhugeint::TryCast(uhugeint_t input, int8_t &result);
template <>
DUCKDB_API bool Uhugeint::TryCast(uhugeint_t input, int16_t &result);
template <>
DUCKDB_API bool Uhugeint::TryCast(uhugeint_t input, int32_t &result);
template <>
DUCKDB_API bool Uhugeint::TryCast(uhugeint_t input, int64_t &result);
template <>
DUCKDB_API bool Uhugeint::TryCast(uhugeint_t input, uint8_t &result);
template <>
DUCKDB_API bool Uhugeint::TryCast(uhugeint_t input, uint16_t &result);
template <>
DUCKDB_API bool Uhugeint::TryCast(uhugeint_t input, uint32_t &result);
template <>
DUCKDB_API bool Uhugeint::TryCast(uhugeint_t input, uint64_t &result);
template <>
DUCKDB_API bool Uhugeint::TryCast(uhugeint_t input, hugeint_t &result);
template <>
DUCKDB_API bool Uhugeint::TryCast(uhugeint_t input, uhugeint_t &result);
template <>
DUCKDB_API bool Uhugeint::TryCast(uhugeint_t input, float &result);
template <>
DUCKDB_API bool Uhugeint::TryCast(uhugeint_t input, double &result);

template <>
bool Uhugeint::TryConvert(int8_t value, uhugeint_t &result);
template <>
bool Uhugeint::TryConvert(int16_t value, uhugeint_t &result);
template <>
bool Uhugeint::TryConvert(int32_t value, uhugeint_t &result);
template <>
bool Uhugeint::TryConvert(int64_t value, uhugeint_t &result);
template <>
bool Uhugeint::TryConvert(uint8_t value, uhugeint_t &result);
template <>
bool Uhugeint::TryConvert(uint16_t value, uhugeint_t &result);
template <>
bool Uhugeint::TryConvert(uint32_t value, uhugeint_t &result);
template <>
bool Uhugeint::TryConvert(uint64_t value, uhugeint_t &result);
template <>
bool Uhugeint::TryConvert(float value, uhugeint_t &result);
template <>
bool Uhugeint::TryConvert(double value, uhugeint_t &result);
template <>
bool Uhugeint::TryConvert(long double value, uhugeint_t &result);
template <>
bool Uhugeint::TryConvert(const char *value, uhugeint_t &result);

} // namespace duckdb
