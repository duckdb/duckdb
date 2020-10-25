//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/add.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct AddOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return left + right;
	}
};

template <> float AddOperator::Operation(float left, float right);
template <> double AddOperator::Operation(double left, double right);
template <> interval_t AddOperator::Operation(interval_t left, interval_t right);
template <> date_t AddOperator::Operation(date_t left, interval_t right);
template <> date_t AddOperator::Operation(interval_t left, date_t right);
template <> timestamp_t AddOperator::Operation(timestamp_t left, interval_t right);
template <> timestamp_t AddOperator::Operation(interval_t left, timestamp_t right);

struct AddOperatorOverflowCheck {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		throw InternalException("Unimplemented type for AddOperatorOverflowCheck");
	}
};

template <> int8_t AddOperatorOverflowCheck::Operation(int8_t left, int8_t right);
template <> int16_t AddOperatorOverflowCheck::Operation(int16_t left, int16_t right);
template <> int32_t AddOperatorOverflowCheck::Operation(int32_t left, int32_t right);
template <> int64_t AddOperatorOverflowCheck::Operation(int64_t left, int64_t right);

struct AddTimeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right);
};

template <> dtime_t AddTimeOperator::Operation(dtime_t left, interval_t right);
template <> dtime_t AddTimeOperator::Operation(interval_t left, dtime_t right);

}
