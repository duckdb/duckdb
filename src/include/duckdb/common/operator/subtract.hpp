//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/subtract.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct SubtractOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return left - right;
	}
};

template <> float SubtractOperator::Operation(float left, float right);
template <> double SubtractOperator::Operation(double left, double right);
template <> interval_t SubtractOperator::Operation(interval_t left, interval_t right);
template <> date_t SubtractOperator::Operation(date_t left, interval_t right);
template <> timestamp_t SubtractOperator::Operation(timestamp_t left, interval_t right);
template <> interval_t SubtractOperator::Operation(timestamp_t left, timestamp_t right);

struct TrySubtractOperator {
	template <class TA, class TB, class TR> static inline bool Operation(TA left, TB right, TR &result) {
		throw InternalException("Unimplemented type for TrySubtractOperator");
	}
};

template <> bool TrySubtractOperator::Operation(int8_t left, int8_t right, int8_t &result);
template <> bool TrySubtractOperator::Operation(int16_t left, int16_t right, int16_t &result);
template <> bool TrySubtractOperator::Operation(int32_t left, int32_t right, int32_t &result);
template <> bool TrySubtractOperator::Operation(int64_t left, int64_t right, int64_t &result);

struct SubtractOperatorOverflowCheck {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		TR result;
		if (!TrySubtractOperator::Operation(left, right, result)) {
			throw OutOfRangeException("Overflow in subtraction of %s (%d - %d)!", TypeIdToString(GetTypeId<TA>()), left, right);
		}
		return result;
	}
};

struct DecimalSubtractOperatorOverflowCheck {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		throw InternalException("Unimplemented type for DecimalSubtractOperatorOverflowCheck");
	}
};

template <> int64_t DecimalSubtractOperatorOverflowCheck::Operation(int64_t left, int64_t right);
template <> hugeint_t DecimalSubtractOperatorOverflowCheck::Operation(hugeint_t left, hugeint_t right);

struct SubtractTimeOperator {
	template <class TA, class TB, class TR> static TR Operation(TA left, TB right);
};

template <> dtime_t SubtractTimeOperator::Operation(dtime_t left, interval_t right);

}
