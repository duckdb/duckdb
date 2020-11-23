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

struct TryAddOperator {
	template <class TA, class TB, class TR> static inline bool Operation(TA left, TB right, TR &result) {
		throw InternalException("Unimplemented type for TryAddOperator");
	}
};

template <> bool TryAddOperator::Operation(int8_t left, int8_t right, int8_t &result);
template <> bool TryAddOperator::Operation(int16_t left, int16_t right, int16_t &result);
template <> bool TryAddOperator::Operation(int32_t left, int32_t right, int32_t &result);
template <> bool TryAddOperator::Operation(int64_t left, int64_t right, int64_t &result);

struct AddOperatorOverflowCheck {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		TR result;
		if (!TryAddOperator::Operation(left, right, result)) {
			throw OutOfRangeException("Overflow in addition of %s (%d + %d)!", TypeIdToString(GetTypeId<TA>()), left,
			                          right);
		}
		return result;
	}
};

struct TryDecimalAdd {
	template <class TA, class TB, class TR> static inline bool Operation(TA left, TB right, TR &result) {
		throw InternalException("Unimplemented type for TryDecimalAdd");
	}
};

template <> bool TryDecimalAdd::Operation(int64_t left, int64_t right, int64_t &result);
template <> bool TryDecimalAdd::Operation(hugeint_t left, hugeint_t right, hugeint_t &result);

struct DecimalAddOverflowCheck {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		TR result;
		if (!TryDecimalAdd::Operation<TA, TB, TR>(left, right, result)) {
			throw OutOfRangeException("Overflow in addition of DECIMAL(18) (%d + %d). You might want to add an "
			                          "explicit cast to a bigger decimal.",
			                          left, right);
		}
		return result;
	}
};

template <> hugeint_t DecimalAddOverflowCheck::Operation(hugeint_t left, hugeint_t right);

struct AddTimeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right);
};

template <> dtime_t AddTimeOperator::Operation(dtime_t left, interval_t right);
template <> dtime_t AddTimeOperator::Operation(interval_t left, dtime_t right);

} // namespace duckdb
