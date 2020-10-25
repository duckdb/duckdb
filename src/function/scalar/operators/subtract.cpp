#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/operator/add.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/value.hpp"

#include "duckdb/common/types/interval.hpp"

using namespace std;

namespace duckdb {

//===--------------------------------------------------------------------===//
// - [subtract]
//===--------------------------------------------------------------------===//
template <> float SubtractOperator::Operation(float left, float right) {
	auto result = left - right;
	if (!Value::FloatIsValid(result)) {
		throw OutOfRangeException("Overflow in subtraction of float!");
	}
	return result;
}

template <> double SubtractOperator::Operation(double left, double right) {
	auto result = left - right;
	if (!Value::DoubleIsValid(result)) {
		throw OutOfRangeException("Overflow in subtraction of double!");
	}
	return result;
}

template <> interval_t SubtractOperator::Operation(interval_t left, interval_t right) {
	interval_t result;
	result.months = left.months - right.months;
	result.days = left.days - right.days;
	result.msecs = left.msecs - right.msecs;
	return result;
}

template <> date_t SubtractOperator::Operation(date_t left, interval_t right) {
	right.months = -right.months;
	right.days = -right.days;
	right.msecs = -right.msecs;
	return AddOperator::Operation<date_t, interval_t, date_t>(left, right);
}

template <> timestamp_t SubtractOperator::Operation(timestamp_t left, interval_t right) {
	right.months = -right.months;
	right.days = -right.days;
	right.msecs = -right.msecs;
	return AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(left, right);
}

template <> interval_t SubtractOperator::Operation(timestamp_t left, timestamp_t right) {
	return Interval::GetDifference(left, right);
}

//===--------------------------------------------------------------------===//
// - [subtract] with overflow check
//===--------------------------------------------------------------------===//
struct OverflowCheckedSubtract {
	template<class SRCTYPE, class UTYPE>
	static inline SRCTYPE Operation(SRCTYPE left, SRCTYPE right) {
		UTYPE result = SubtractOperator::Operation<UTYPE, UTYPE, UTYPE>(UTYPE(left), UTYPE(right));
		if (result < NumericLimits<SRCTYPE>::Minimum() || result > NumericLimits<SRCTYPE>::Maximum()) {
			throw OutOfRangeException("Overflow in subtract of %s (%d - %d = %d)!", TypeIdToString(GetTypeId<SRCTYPE>()), left, right, result);
		}
		return result;
	}
};

template <> int8_t SubtractOperatorOverflowCheck::Operation(int8_t left, int8_t right) {
	return OverflowCheckedSubtract::Operation<int8_t, int16_t>(left, right);
}

template <> int16_t SubtractOperatorOverflowCheck::Operation(int16_t left, int16_t right) {
	return OverflowCheckedSubtract::Operation<int16_t, int32_t>(left, right);
}

template <> int32_t SubtractOperatorOverflowCheck::Operation(int32_t left, int32_t right) {
	return OverflowCheckedSubtract::Operation<int32_t, int64_t>(left, right);
}

template <> int64_t SubtractOperatorOverflowCheck::Operation(int64_t left, int64_t right) {
#if defined(__GNUC__) || defined(__clang__)
	int64_t result;
	if (__builtin_sub_overflow(left, right, &result)) {
		throw OutOfRangeException("Overflow in subtract of BIGINT (%d - %d);", left, right);
	}
	// FIXME: this check can be removed if we get rid of NullValue<T>
	if (result == std::numeric_limits<int64_t>::min()) {
		throw OutOfRangeException("Overflow in subtract of BIGINT (%d - %d);", left, right);
	}
	return result;
#else
	if (right < 0) {
		if (NumericLimits<int64_t>::Maximum() + right < left) {
			throw OutOfRangeException("Overflow in subtract of BIGINT (%d - %d);", left, right);
		}
	} else {
		if (NumericLimits<int64_t>::Minimum() + right > left) {
			throw OutOfRangeException("Overflow in subtract of BIGINT (%d - %d);", left, right);
		}
	}
	return left - right;
#endif
}

//===--------------------------------------------------------------------===//
// subtract time operator
//===--------------------------------------------------------------------===//
template <> dtime_t SubtractTimeOperator::Operation(dtime_t left, interval_t right) {
	right.msecs = -right.msecs;
	return AddTimeOperator::Operation<dtime_t, interval_t, dtime_t>(left, right);
}

}
