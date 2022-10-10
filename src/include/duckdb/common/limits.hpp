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
	DUCKDB_API static T Minimum();
	DUCKDB_API static T Maximum();
	DUCKDB_API static bool IsSigned();
	DUCKDB_API static idx_t Digits();
};

template <>
struct NumericLimits<int8_t> {
	DUCKDB_API static int8_t Minimum();
	DUCKDB_API static int8_t Maximum();
	DUCKDB_API static bool IsSigned() {
		return true;
	}
	DUCKDB_API static idx_t Digits() {
		return 3;
	}
};
template <>
struct NumericLimits<int16_t> {
	DUCKDB_API static int16_t Minimum();
	DUCKDB_API static int16_t Maximum();
	DUCKDB_API static bool IsSigned() {
		return true;
	}
	DUCKDB_API static idx_t Digits() {
		return 5;
	}
};
template <>
struct NumericLimits<int32_t> {
	DUCKDB_API static int32_t Minimum();
	DUCKDB_API static int32_t Maximum();
	DUCKDB_API static bool IsSigned() {
		return true;
	}
	DUCKDB_API static idx_t Digits() {
		return 10;
	}
};
template <>
struct NumericLimits<int64_t> {
	DUCKDB_API static int64_t Minimum();
	DUCKDB_API static int64_t Maximum();
	DUCKDB_API static bool IsSigned() {
		return true;
	}
	DUCKDB_API static idx_t Digits() {
		return 19;
	}
};
template <>
struct NumericLimits<hugeint_t> {
	DUCKDB_API static hugeint_t Minimum();
	DUCKDB_API static hugeint_t Maximum();
	DUCKDB_API static bool IsSigned() {
		return true;
	}
	DUCKDB_API static idx_t Digits() {
		return 39;
	}
};
template <>
struct NumericLimits<uint8_t> {
	DUCKDB_API static uint8_t Minimum();
	DUCKDB_API static uint8_t Maximum();
	DUCKDB_API static bool IsSigned() {
		return false;
	}
	DUCKDB_API static idx_t Digits() {
		return 3;
	}
};
template <>
struct NumericLimits<uint16_t> {
	DUCKDB_API static uint16_t Minimum();
	DUCKDB_API static uint16_t Maximum();
	DUCKDB_API static bool IsSigned() {
		return false;
	}
	DUCKDB_API static idx_t Digits() {
		return 5;
	}
};
template <>
struct NumericLimits<uint32_t> {
	DUCKDB_API static uint32_t Minimum();
	DUCKDB_API static uint32_t Maximum();
	DUCKDB_API static bool IsSigned() {
		return false;
	}
	DUCKDB_API static idx_t Digits() {
		return 10;
	}
};
template <>
struct NumericLimits<uint64_t> {
	DUCKDB_API static uint64_t Minimum();
	DUCKDB_API static uint64_t Maximum();
	DUCKDB_API static bool IsSigned() {
		return false;
	}
	DUCKDB_API static idx_t Digits() {
		return 20;
	}
};
template <>
struct NumericLimits<float> {
	DUCKDB_API static float Minimum();
	DUCKDB_API static float Maximum();
	DUCKDB_API static bool IsSigned() {
		return true;
	}
	DUCKDB_API static idx_t Digits() {
		return 127;
	}
};
template <>
struct NumericLimits<double> {
	DUCKDB_API static double Minimum();
	DUCKDB_API static double Maximum();
	DUCKDB_API static bool IsSigned() {
		return true;
	}
	DUCKDB_API static idx_t Digits() {
		return 250;
	}
};

} // namespace duckdb
