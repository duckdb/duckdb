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

struct SubtractOperatorOverflowCheck {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		throw InternalException("Unimplemented type for SubtractOperatorOverflowCheck");
	}
};

struct DecimalSubtractOperatorOverflowCheck {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		throw InternalException("Unimplemented type for DecimalSubtractOperatorOverflowCheck");
	}
};

template <> int64_t DecimalSubtractOperatorOverflowCheck::Operation(int64_t left, int64_t right);
template <> hugeint_t DecimalSubtractOperatorOverflowCheck::Operation(hugeint_t left, hugeint_t right);

template <> int8_t SubtractOperatorOverflowCheck::Operation(int8_t left, int8_t right);
template <> int16_t SubtractOperatorOverflowCheck::Operation(int16_t left, int16_t right);
template <> int32_t SubtractOperatorOverflowCheck::Operation(int32_t left, int32_t right);
template <> int64_t SubtractOperatorOverflowCheck::Operation(int64_t left, int64_t right);

struct SubtractTimeOperator {
	template <class TA, class TB, class TR> static TR Operation(TA left, TB right);
};

template <> dtime_t SubtractTimeOperator::Operation(dtime_t left, interval_t right);

}
