//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/hugeint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

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

	template<class T>
	static bool TryCast(hugeint_t input, T &result);

	template<class T>
	static hugeint_t Convert(T value);

	static hugeint_t Add(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Subtract(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Multiply(hugeint_t lhs, hugeint_t rhs);

	static bool Equals(hugeint_t lhs, hugeint_t rhs) {
		return lhs.lower == rhs.lower && lhs.upper == rhs.upper;
	}
	static bool GreaterThan(hugeint_t lhs, hugeint_t rhs) {
		return lhs.upper > rhs.upper || (lhs.upper == rhs.upper && lhs.lower > rhs.lower);
	}
	static bool GreaterThanEquals(hugeint_t lhs, hugeint_t rhs) {
		return lhs.upper > rhs.upper || (lhs.upper == rhs.upper && lhs.lower >= rhs.lower);
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
