#include "duckdb/common/operator/add.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/value.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"

using namespace std;

namespace duckdb {

//===--------------------------------------------------------------------===//
// + [add]
//===--------------------------------------------------------------------===//
template <> float AddOperator::Operation(float left, float right) {
	auto result = left + right;
	if (!Value::FloatIsValid(result)) {
		throw OutOfRangeException("Overflow in addition of float!");
	}
	return result;
}

template <> double AddOperator::Operation(double left, double right) {
	auto result = left + right;
	if (!Value::DoubleIsValid(result)) {
		throw OutOfRangeException("Overflow in addition of double!");
	}
	return result;
}

template <> interval_t AddOperator::Operation(interval_t left, interval_t right) {
	left.months = AddOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(left.months, right.months);
	left.days = AddOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(left.days, right.days);
	left.msecs = AddOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(left.msecs, right.msecs);
	return left;
}

template <> date_t AddOperator::Operation(date_t left, interval_t right) {
	date_t result;
	if (right.months != 0) {
		int32_t year, month, day;
		Date::Convert(left, year, month, day);
		int32_t year_diff = right.months / Interval::MONTHS_PER_YEAR;
		year += year_diff;
		month += right.months - year_diff * Interval::MONTHS_PER_YEAR;
		if (month > Interval::MONTHS_PER_YEAR) {
			year++;
			month -= Interval::MONTHS_PER_YEAR;
		} else if (month <= 0) {
			year--;
			month += Interval::MONTHS_PER_YEAR;
		}
		result = Date::FromDate(year, month, day);
	} else {
		result = left;
	}
	if (right.days != 0) {
		result += right.days;
	}
	if (right.msecs != 0) {
		result += right.msecs / Interval::MSECS_PER_DAY;
	}
	return result;
}

template <> date_t AddOperator::Operation(interval_t left, date_t right) {
	return AddOperator::Operation<date_t, interval_t, date_t>(right, left);
}

template <> timestamp_t AddOperator::Operation(timestamp_t left, interval_t right) {
	auto date = Timestamp::GetDate(left);
	auto time = Timestamp::GetTime(left);
	auto new_date = AddOperator::Operation<date_t, interval_t, date_t>(date, right);
	auto new_time = AddTimeOperator::Operation<dtime_t, interval_t, dtime_t>(time, right);
	return Timestamp::FromDatetime(new_date, new_time);
}

template <> timestamp_t AddOperator::Operation(interval_t left, timestamp_t right) {
	return AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(right, left);
}

//===--------------------------------------------------------------------===//
// + [add] with overflow check
//===--------------------------------------------------------------------===//
struct OverflowCheckedAddition {
	template<class SRCTYPE, class UTYPE>
	static inline SRCTYPE Operation(SRCTYPE left, SRCTYPE right) {
		UTYPE result = AddOperator::Operation<UTYPE, UTYPE, UTYPE>(UTYPE(left), UTYPE(right));
		if (result < NumericLimits<SRCTYPE>::Minimum() || result > NumericLimits<SRCTYPE>::Maximum()) {
			throw OutOfRangeException("Overflow in addition of %s (%d + %d = %d)!", TypeIdToString(GetTypeId<SRCTYPE>()), left, right, result);
		}
		return result;
	}
};

template <> int8_t AddOperatorOverflowCheck::Operation(int8_t left, int8_t right) {
	return OverflowCheckedAddition::Operation<int8_t, int16_t>(left, right);
}

template <> int16_t AddOperatorOverflowCheck::Operation(int16_t left, int16_t right) {
	return OverflowCheckedAddition::Operation<int16_t, int32_t>(left, right);
}

template <> int32_t AddOperatorOverflowCheck::Operation(int32_t left, int32_t right) {
	return OverflowCheckedAddition::Operation<int32_t, int64_t>(left, right);
}

template <> int64_t AddOperatorOverflowCheck::Operation(int64_t left, int64_t right) {
#if defined(__GNUC__) || defined(__clang__)
	int64_t result;
	if (__builtin_add_overflow(left, right, &result)) {
		throw OutOfRangeException("Overflow in addition of BIGINT (%d + %d);", left, right);
	}
#else
	// https://blog.regehr.org/archives/1139
	int64_t result = int64_t((uint64_t)left + (uint64_t)right);
	if ((left < 0 && right < 0 && result >= 0) || (left >= 0 && right >= 0 && result < 0)) {
		throw OutOfRangeException("Overflow in addition of BIGINT (%d + %d);", left, right);
	}
#endif
	// FIXME: this check can be removed if we get rid of NullValue<T>
	if (result == std::numeric_limits<int64_t>::min()) {
		throw OutOfRangeException("Overflow in addition of BIGINT (%d + %d);", left, right);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// add time operator
//===--------------------------------------------------------------------===//
template <> dtime_t AddTimeOperator::Operation(dtime_t left, interval_t right) {
	int64_t diff = right.msecs - ((right.msecs / Interval::MSECS_PER_DAY) * Interval::MSECS_PER_DAY);
	left += diff;
	if (left >= Interval::MSECS_PER_DAY) {
		left -= Interval::MSECS_PER_DAY;
	} else if (left < 0) {
		left += Interval::MSECS_PER_DAY;
	}
	return left;
}

template <> dtime_t AddTimeOperator::Operation(interval_t left, dtime_t right) {
	return AddTimeOperator::Operation<dtime_t, interval_t, dtime_t>(right, left);
}

}
