//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/subtract.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arch.h"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct SET_ARCH(SubtractOperator) {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return left - right;
	}
};

template <>
float SET_ARCH(SubtractOperator)::Operation(float left, float right);
template <>
double SET_ARCH(SubtractOperator)::Operation(double left, double right);
template <>
interval_t SET_ARCH(SubtractOperator)::Operation(interval_t left, interval_t right);
template <>
date_t SET_ARCH(SubtractOperator)::Operation(date_t left, interval_t right);
template <>
timestamp_t SET_ARCH(SubtractOperator)::Operation(timestamp_t left, interval_t right);
template <>
interval_t SET_ARCH(SubtractOperator)::Operation(timestamp_t left, timestamp_t right);

struct SET_ARCH(TrySubtractOperator) {
	template <class TA, class TB, class TR>
	static inline bool Operation(TA left, TB right, TR &result) {
		throw InternalException("Unimplemented type for TrySubtractOperator");
	}
};

template <>
bool SET_ARCH(TrySubtractOperator)::Operation(uint8_t left, uint8_t right, uint8_t &result);
template <>
bool SET_ARCH(TrySubtractOperator)::Operation(uint16_t left, uint16_t right, uint16_t &result);
template <>
bool SET_ARCH(TrySubtractOperator)::Operation(uint32_t left, uint32_t right, uint32_t &result);
template <>
bool SET_ARCH(TrySubtractOperator)::Operation(uint64_t left, uint64_t right, uint64_t &result);

template <>
bool SET_ARCH(TrySubtractOperator)::Operation(int8_t left, int8_t right, int8_t &result);
template <>
bool SET_ARCH(TrySubtractOperator)::Operation(int16_t left, int16_t right, int16_t &result);
template <>
bool SET_ARCH(TrySubtractOperator)::Operation(int32_t left, int32_t right, int32_t &result);
template <>
bool SET_ARCH(TrySubtractOperator)::Operation(int64_t left, int64_t right, int64_t &result);

struct SubtractOperatorOverflowCheck {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		TR result;
		if (!SET_ARCH(TrySubtractOperator)::Operation(left, right, result)) {
			throw OutOfRangeException("Overflow in subtraction of %s (%d - %d)!", TypeIdToString(GetTypeId<TA>()), left,
			                          right);
		}
		return result;
	}
};

struct SET_ARCH(TryDecimalSubtract) {
	template <class TA, class TB, class TR>
	static inline bool Operation(TA left, TB right, TR &result) {
		throw InternalException("Unimplemented type for TryDecimalSubtract");
	}
};

template <>
bool SET_ARCH(TryDecimalSubtract)::Operation(int16_t left, int16_t right, int16_t &result);
template <>
bool SET_ARCH(TryDecimalSubtract)::Operation(int32_t left, int32_t right, int32_t &result);
template <>
bool SET_ARCH(TryDecimalSubtract)::Operation(int64_t left, int64_t right, int64_t &result);
template <>
bool SET_ARCH(TryDecimalSubtract)::Operation(hugeint_t left, hugeint_t right, hugeint_t &result);

struct SET_ARCH(DecimalSubtractOverflowCheck) {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		TR result;
		if (!SET_ARCH(TryDecimalSubtract)::Operation<TA, TB, TR>(left, right, result)) {
			throw OutOfRangeException("Overflow in subtract of DECIMAL(18) (%d - %d). You might want to add an "
			                          "explicit cast to a bigger decimal.",
			                          left, right);
		}
		return result;
	}
};

template <>
hugeint_t SET_ARCH(DecimalSubtractOverflowCheck)::Operation(hugeint_t left, hugeint_t right);

struct SET_ARCH(SubtractTimeOperator) {
	template <class TA, class TB, class TR>
	static TR Operation(TA left, TB right);
};

template <>
dtime_t SET_ARCH(SubtractTimeOperator)::Operation(dtime_t left, interval_t right);

} // namespace duckdb
