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
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

//	Linear interpolation between two values
struct InterpolateOperator {
	template <typename TARGET_TYPE>
	static inline TARGET_TYPE Operation(const TARGET_TYPE &lo, const double numerator, const double denominator,
	                                    const TARGET_TYPE &hi) {
		if constexpr (std::is_integral_v<TARGET_TYPE>) {
			// exact rational interpolation: dividing to DOUBLE first
			// can drop an exactly-integral result just below its boundary
			return InterpolateIntegerRatio<TARGET_TYPE>(lo, numerator, denominator, hi);
		} else {
			return Operation<TARGET_TYPE>(lo, numerator / denominator, hi);
		}
	}

	template <typename TARGET_TYPE>
	static inline TARGET_TYPE Operation(const TARGET_TYPE &lo, const double d, const TARGET_TYPE &hi) {
		const auto delta = static_cast<double>(hi) - static_cast<double>(lo);
		const auto result = static_cast<double>(lo) + delta * d;
		// casting an out-of-range double to an integer is UB and platform-dependent
		// (x86 cvttsd2si yields the integer indefinite value, ARM fcvtzs saturates) - clamp first.
		// for uint64 the bound rounds up to exactly 2^64 in double, so every
		// representable-in-double in-range value still passes through unchanged.
		if (result >= static_cast<double>(NumericLimits<TARGET_TYPE>::Maximum())) {
			return NumericLimits<TARGET_TYPE>::Maximum();
		}
		if (result <= static_cast<double>(NumericLimits<TARGET_TYPE>::Minimum())) {
			return NumericLimits<TARGET_TYPE>::Minimum();
		}
		// integer targets round toward the lower endpoint, matching the exact int64 paths -
		// a plain cast truncates toward zero, which rounds negative values up
		if constexpr (std::is_integral_v<TARGET_TYPE>) {
			return LossyNumericCast<TARGET_TYPE>(std::floor(result));
		}
		return LossyNumericCast<TARGET_TYPE>(result);
	}

private:
	template <typename TARGET_TYPE>
	static inline hugeint_t ToHugeint(const TARGET_TYPE &value) {
		if constexpr (std::is_unsigned_v<TARGET_TYPE>) {
			return hugeint_t(0, static_cast<uint64_t>(value));
		} else {
			return hugeint_t(static_cast<int64_t>(value));
		}
	}

	//	Interpolate from the exact ratio, so results that are integral in exact arithmetic
	//	stay integral. Floors the offset toward the lower endpoint.
	template <typename TARGET_TYPE>
	static inline TARGET_TYPE InterpolateIntegerRatio(const TARGET_TYPE &lo, double numerator, double denominator,
	                                                  const TARGET_TYPE &hi) {
		D_ASSERT(std::isfinite(numerator) && std::isfinite(denominator) && denominator != 0);
		if (denominator < 0) {
			numerator = -numerator;
			denominator = -denominator;
		}
		if (numerator <= 0 || lo == hi) {
			return lo;
		}
		if (numerator >= denominator) {
			return hi;
		}
		const auto delta = ToHugeint(hi) - ToHugeint(lo);
		const auto descending = delta < 0;
		const auto distance = descending ? -delta : delta;

		//	fast path: integral ratios (e.g. row positions) that fit native INT64 arithmetic
		constexpr auto int64_max = std::numeric_limits<int64_t>::max();
		if (numerator <= static_cast<double>(int64_max) && denominator <= static_cast<double>(int64_max) &&
		    numerator == static_cast<double>(static_cast<int64_t>(numerator)) &&
		    denominator == static_cast<double>(static_cast<int64_t>(denominator))) {
			const auto num = static_cast<int64_t>(numerator);
			const auto den = static_cast<int64_t>(denominator);
			if (distance <= hugeint_t(int64_max / num)) {
				const auto scaled = Hugeint::Cast<int64_t>(distance) * num;
				auto offset = scaled / den;
				if (descending && scaled % den != 0) {
					offset += 1;
				}
				return Hugeint::Cast<TARGET_TYPE>(ToHugeint(lo) + (descending ? -offset : offset));
			}
		}

		//	decompose the ratio exactly; the product needs at most 117 bits.
		int32_t numerator_exponent;
		int32_t denominator_exponent;
		const auto numerator_fraction = std::frexp(numerator, &numerator_exponent);
		const auto denominator_fraction = std::frexp(denominator, &denominator_exponent);
		constexpr auto digits = std::numeric_limits<double>::digits;
		const auto numerator_significand = static_cast<int64_t>(std::ldexp(numerator_fraction, digits));
		const auto denominator_significand = static_cast<int64_t>(std::ldexp(denominator_fraction, digits));
		const auto shift = denominator_exponent - numerator_exponent;
		D_ASSERT(shift >= 0);
		const auto product = distance * numerator_significand;
		//	larger gaps make the denominator exceed the 117-bit product
		if (shift >= 65) {
			return descending && lo > NumericLimits<TARGET_TYPE>::Minimum() ? TARGET_TYPE(lo - 1) : lo;
		}

		const auto divisor = hugeint_t(denominator_significand) << shift;
		hugeint_t remainder;
		auto offset = Hugeint::DivMod(product, divisor, remainder);
		if (descending && remainder != 0) {
			offset += 1;
		}
		return Hugeint::Cast<TARGET_TYPE>(ToHugeint(lo) + (descending ? -offset : offset));
	}
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
