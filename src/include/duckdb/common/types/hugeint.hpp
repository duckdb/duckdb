//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/hugeint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

#include <limits>

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

	template<class T>
	static bool TryCast(hugeint_t input, T &result);

	template<class T>
	static hugeint_t Convert(T value);

	static void NegateInPlace(hugeint_t &input) {
		input.lower = std::numeric_limits<uint64_t>::max() - input.lower + 1;
		input.upper = -1 - input.upper;
	}
	static hugeint_t Negate(hugeint_t input) {
		NegateInPlace(input);
		return input;
	}

	static hugeint_t Add(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Subtract(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Multiply(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Divide(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Modulo(hugeint_t lhs, hugeint_t rhs);

	static void AddInPlace(hugeint_t &lhs, hugeint_t rhs);
	static void SubtractInPlace(hugeint_t &lhs, hugeint_t rhs);

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
};

template<> bool Hugeint::TryCast(hugeint_t input, int8_t &result);
template<> bool Hugeint::TryCast(hugeint_t input, int16_t &result);
template<> bool Hugeint::TryCast(hugeint_t input, int32_t &result);
template<> bool Hugeint::TryCast(hugeint_t input, int64_t &result);
template<> bool Hugeint::TryCast(hugeint_t input, float &result);
template<> bool Hugeint::TryCast(hugeint_t input, double &result);

template<> hugeint_t Hugeint::Convert(int8_t value);
template<> hugeint_t Hugeint::Convert(int16_t value);
template<> hugeint_t Hugeint::Convert(int32_t value);
template<> hugeint_t Hugeint::Convert(int64_t value);
template<> hugeint_t Hugeint::Convert(float value);
template<> hugeint_t Hugeint::Convert(double value);
} // namespace duckdb
