//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/limits.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

template <class T>
struct NumericLimits {
	static T Minimum();
	static T Maximum();
	static bool IsSigned();
	static idx_t Digits();
};

template <>
struct NumericLimits<int8_t> {
	static int8_t Minimum();
	static int8_t Maximum();
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 3;
	}
};
template <>
struct NumericLimits<int16_t> {
	static int16_t Minimum();
	static int16_t Maximum();
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 5;
	}
};
template <>
struct NumericLimits<int32_t> {
	static int32_t Minimum();
	static int32_t Maximum();
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 10;
	}
};
template <>
struct NumericLimits<int64_t> {
	static int64_t Minimum();
	static int64_t Maximum();
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 19;
	}
};
template <>
struct NumericLimits<hugeint_t> {
	static hugeint_t Minimum();
	static hugeint_t Maximum();
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 39;
	}
};
template <>
struct NumericLimits<uint8_t> {
	static uint8_t Minimum();
	static uint8_t Maximum();
	static bool IsSigned() {
		return false;
	}
	static idx_t Digits() {
		return 3;
	}
};
template <>
struct NumericLimits<uint16_t> {
	static uint16_t Minimum();
	static uint16_t Maximum();
	static bool IsSigned() {
		return false;
	}
	static idx_t Digits() {
		return 5;
	}
};
template <>
struct NumericLimits<uint32_t> {
	static uint32_t Minimum();
	static uint32_t Maximum();
	static bool IsSigned() {
		return false;
	}
	static idx_t Digits() {
		return 10;
	}
};
template <>
struct NumericLimits<uint64_t> {
	static uint64_t Minimum();
	static uint64_t Maximum();
	static bool IsSigned() {
		return false;
	}
	static idx_t Digits() {
		return 20;
	}
};
template <>
struct NumericLimits<float> {
	static float Minimum();
	static float Maximum();
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 127;
	}
};
template <>
struct NumericLimits<double> {
	static double Minimum();
	static double Maximum();
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 250;
	}
};

} // namespace duckdb
