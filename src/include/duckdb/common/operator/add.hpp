//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/add.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arch.h"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct SET_ARCH(AddOperator) {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return left + right;
	}
};

template <>
float SET_ARCH(AddOperator)::Operation(float left, float right);
template <>
double SET_ARCH(AddOperator)::Operation(double left, double right);
template <>
interval_t SET_ARCH(AddOperator)::Operation(interval_t left, interval_t right);
template <>
date_t SET_ARCH(AddOperator)::Operation(date_t left, interval_t right);
template <>
date_t SET_ARCH(AddOperator)::Operation(interval_t left, date_t right);
template <>
timestamp_t SET_ARCH(AddOperator)::Operation(timestamp_t left, interval_t right);
template <>
timestamp_t SET_ARCH(AddOperator)::Operation(interval_t left, timestamp_t right);

struct SET_ARCH(TryAddOperator) {
	template <class TA, class TB, class TR>
	static inline bool Operation(TA left, TB right, TR &result) {
		throw InternalException("Unimplemented type for TryAddOperator");
	}
};

template <>
bool SET_ARCH(TryAddOperator)::Operation(uint8_t left, uint8_t right, uint8_t &result);
template <>
bool SET_ARCH(TryAddOperator)::Operation(uint16_t left, uint16_t right, uint16_t &result);
template <>
bool SET_ARCH(TryAddOperator)::Operation(uint32_t left, uint32_t right, uint32_t &result);
template <>
bool SET_ARCH(TryAddOperator)::Operation(uint64_t left, uint64_t right, uint64_t &result);

template <>
bool SET_ARCH(TryAddOperator)::Operation(int8_t left, int8_t right, int8_t &result);
template <>
bool SET_ARCH(TryAddOperator)::Operation(int16_t left, int16_t right, int16_t &result);
template <>
bool SET_ARCH(TryAddOperator)::Operation(int32_t left, int32_t right, int32_t &result);
template <>
bool SET_ARCH(TryAddOperator)::Operation(int64_t left, int64_t right, int64_t &result);

struct AddOperatorOverflowCheck {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		TR result;
		if (!SET_ARCH(TryAddOperator)::Operation(left, right, result)) {
			throw OutOfRangeException("Overflow in addition of %s (%d + %d)!", TypeIdToString(GetTypeId<TA>()), left,
			                          right);
		}
		return result;
	}
};

struct SET_ARCH(TryDecimalAdd) {
	template <class TA, class TB, class TR>
	static inline bool Operation(TA left, TB right, TR &result) {
		throw InternalException("Unimplemented type for TryDecimalAdd");
	}
};

template <>
bool SET_ARCH(TryDecimalAdd)::Operation(int16_t left, int16_t right, int16_t &result);
template <>
bool SET_ARCH(TryDecimalAdd)::Operation(int32_t left, int32_t right, int32_t &result);
template <>
bool SET_ARCH(TryDecimalAdd)::Operation(int64_t left, int64_t right, int64_t &result);
template <>
bool SET_ARCH(TryDecimalAdd)::Operation(hugeint_t left, hugeint_t right, hugeint_t &result);

struct SET_ARCH(DecimalAddOverflowCheck) {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		TR result;
		if (!SET_ARCH(TryDecimalAdd)::Operation<TA, TB, TR>(left, right, result)) {
			throw OutOfRangeException("Overflow in addition of DECIMAL(18) (%d + %d). You might want to add an "
			                          "explicit cast to a bigger decimal.",
			                          left, right);
		}
		return result;
	}
};

template <>
hugeint_t SET_ARCH(DecimalAddOverflowCheck)::Operation(hugeint_t left, hugeint_t right);

struct SET_ARCH(AddTimeOperator) {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right);
};

template <>
dtime_t SET_ARCH(AddTimeOperator)::Operation(dtime_t left, interval_t right);
template <>
dtime_t SET_ARCH(AddTimeOperator)::Operation(interval_t left, dtime_t right);

} // namespace duckdb
