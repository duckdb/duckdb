//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/interpolate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

//	Linear interpolation between two values
struct InterpolateOperator {
	template <typename TARGET_TYPE>
	static inline TARGET_TYPE Operation(const TARGET_TYPE &lo, const double numerator, const double denominator,
	                                    const TARGET_TYPE &hi) {
		if constexpr (IsInteger<TARGET_TYPE>()) {
			return InterpolateInteger(lo, numerator, denominator, hi);
		} else {
			return Operation<TARGET_TYPE>(lo, numerator / denominator, hi);
		}
	}

	template <typename TARGET_TYPE>
	static inline TARGET_TYPE Operation(const TARGET_TYPE &lo, const uhugeint_t &numerator,
	                                    const uhugeint_t &denominator, const TARGET_TYPE &hi) {
		static_assert(IsInteger<TARGET_TYPE>(), "Exact ratios require an integer target");
		return InterpolateInteger(lo, numerator, denominator, hi);
	}

	template <typename TARGET_TYPE>
	static inline TARGET_TYPE Operation(const TARGET_TYPE &lo, const double d, const TARGET_TYPE &hi) {
		if constexpr (IsInteger<TARGET_TYPE>()) {
			return Operation(lo, d, 1.0, hi);
		} else {
			const auto delta = static_cast<double>(hi) - static_cast<double>(lo);
			return LossyNumericCast<TARGET_TYPE>(static_cast<double>(lo) + delta * d);
		}
	}

private:
	template <typename T>
	static constexpr bool IsInteger() {
		return std::is_integral_v<T> || std::is_same_v<T, hugeint_t> || std::is_same_v<T, uhugeint_t>;
	}

	template <typename T>
	static inline uhugeint_t ToOrderedInteger(const T &value) {
		if constexpr (std::is_same_v<T, uhugeint_t>) {
			return value;
		} else if constexpr (std::is_unsigned_v<T>) {
			return uhugeint_t(0, static_cast<uint64_t>(value));
		} else {
			const auto integer = hugeint_t(value);
			return uhugeint_t(static_cast<uint64_t>(integer.upper) ^ (uint64_t(1) << 63), integer.lower);
		}
	}

	template <typename T>
	static inline T FromOrderedInteger(const uhugeint_t &value) {
		if constexpr (std::is_same_v<T, uhugeint_t>) {
			return value;
		} else if constexpr (std::is_unsigned_v<T>) {
			return Uhugeint::Cast<T>(value);
		} else {
			const auto integer = hugeint_t(static_cast<int64_t>(value.upper ^ (uint64_t(1) << 63)), value.lower);
			return Hugeint::Cast<T>(integer);
		}
	}

	template <typename TARGET_TYPE, typename RATIO_TYPE>
	static inline TARGET_TYPE InterpolateInteger(const TARGET_TYPE &lo, const RATIO_TYPE &numerator,
	                                             const RATIO_TYPE &denominator, const TARGET_TYPE &hi) {
		const auto first = ToOrderedInteger(lo);
		const auto last = ToOrderedInteger(hi);
		const auto descending = last < first;
		const auto distance = descending ? first - last : last - first;
		const auto offset = IntegerOffset(distance, numerator, denominator, descending);
		return FromOrderedInteger<TARGET_TYPE>(descending ? first - offset : first + offset);
	}

	static uhugeint_t IntegerOffset(const uhugeint_t &distance, double numerator, double denominator, bool descending);
	static uhugeint_t IntegerOffset(const uhugeint_t &distance, const uhugeint_t &numerator,
	                                const uhugeint_t &denominator, bool descending);
};

template <>
double InterpolateOperator::Operation(const double &lo, const double d, const double &hi);
template <>
int64_t InterpolateOperator::Operation(const int64_t &lo, const double d, const int64_t &hi);
template <>
dtime_t InterpolateOperator::Operation(const dtime_t &lo, const double d, const dtime_t &hi);
template <>
dtime_tz_t InterpolateOperator::Operation(const dtime_tz_t &lo, const double d, const dtime_tz_t &hi);
template <>
timestamp_t InterpolateOperator::Operation(const timestamp_t &lo, const double d, const timestamp_t &hi);
template <>
hugeint_t InterpolateOperator::Operation(const hugeint_t &lo, const double d, const hugeint_t &hi);
template <>
uhugeint_t InterpolateOperator::Operation(const uhugeint_t &lo, const double d, const uhugeint_t &hi);
template <>
interval_t InterpolateOperator::Operation(const interval_t &lo, const double d, const interval_t &hi);

} // namespace duckdb
