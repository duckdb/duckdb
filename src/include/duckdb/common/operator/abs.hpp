//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/abs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

struct AbsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return input < 0 ? -input : input;
	}
};

template <>
inline hugeint_t AbsOperator::Operation(hugeint_t input) {
	const hugeint_t zero(0);
	return (input < zero) ? -input : input;
}

struct TryAbsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return AbsOperator::Operation<TA, TR>(input);
	}
};

template <>
inline int8_t TryAbsOperator::Operation(int8_t input) {
	if (input == NumericLimits<int8_t>::Minimum()) {
		throw OutOfRangeException("Overflow on abs(%d)", input);
	}
	return input < 0 ? -input : input;
}

template <>
inline int16_t TryAbsOperator::Operation(int16_t input) {
	if (input == NumericLimits<int16_t>::Minimum()) {
		throw OutOfRangeException("Overflow on abs(%d)", input);
	}
	return input < 0 ? -input : input;
}

template <>
inline int32_t TryAbsOperator::Operation(int32_t input) {
	if (input == NumericLimits<int32_t>::Minimum()) {
		throw OutOfRangeException("Overflow on abs(%d)", input);
	}
	return input < 0 ? -input : input;
}

template <>
inline int64_t TryAbsOperator::Operation(int64_t input) {
	if (input == NumericLimits<int64_t>::Minimum()) {
		throw OutOfRangeException("Overflow on abs(%d)", input);
	}
	return input < 0 ? -input : input;
}

template <>
inline dtime_t TryAbsOperator::Operation(dtime_t input) {
	return dtime_t(TryAbsOperator::Operation<int64_t, int64_t>(input.micros));
}

template <>
inline interval_t TryAbsOperator::Operation(interval_t input) {
	return {TryAbsOperator::Operation<int32_t, int32_t>(input.months),
	        TryAbsOperator::Operation<int32_t, int32_t>(input.days),
	        TryAbsOperator::Operation<int64_t, int64_t>(input.micros)};
}

} // namespace duckdb
