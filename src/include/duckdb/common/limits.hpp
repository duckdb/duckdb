//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/limits.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/types.hpp"

#include <type_traits>

// Undef annoying windows macro
#undef max

#include <limits>

namespace duckdb {

template <class T>
struct NumericLimits {
	static constexpr T Minimum() {
		return std::numeric_limits<T>::lowest();
	}
	static constexpr T Maximum() {
		return std::numeric_limits<T>::max();
	}
	DUCKDB_API static constexpr bool IsSigned() {
		return std::is_signed<T>::value;
	}
	DUCKDB_API static constexpr idx_t Digits();
};

template <>
struct NumericLimits<hugeint_t> {
	static constexpr hugeint_t Minimum() {
		return {std::numeric_limits<int64_t>::lowest(), 1};
	};
	static constexpr hugeint_t Maximum() {
		return {std::numeric_limits<int64_t>::max(), std::numeric_limits<uint64_t>::max()};
	};
	static constexpr bool IsSigned() {
		return true;
	}

	static constexpr idx_t Digits() {
		return 39;
	}
};

template <>
constexpr idx_t NumericLimits<int8_t>::Digits() {
	return 3;
}

template <>
constexpr idx_t NumericLimits<int16_t>::Digits() {
	return 5;
}

template <>
constexpr idx_t NumericLimits<int32_t>::Digits() {
	return 10;
}

template <>
constexpr idx_t NumericLimits<int64_t>::Digits() {
	return 19;
}

template <>
constexpr idx_t NumericLimits<uint8_t>::Digits() {
	return 3;
}

template <>
constexpr idx_t NumericLimits<uint16_t>::Digits() {
	return 5;
}

template <>
constexpr idx_t NumericLimits<uint32_t>::Digits() {
	return 10;
}

template <>
constexpr idx_t NumericLimits<uint64_t>::Digits() {
	return 20;
}

template <>
constexpr idx_t NumericLimits<float>::Digits() {
	return 127;
}

template <>
constexpr idx_t NumericLimits<double>::Digits() {
	return 250;
}

} // namespace duckdb
