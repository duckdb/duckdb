#include "duckdb/common/operator/add.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/value.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/windows_undefs.hpp"

#include <limits>

namespace duckdb {

//===--------------------------------------------------------------------===//
// + [add]
//===--------------------------------------------------------------------===//
template <>
float AddOperator::Operation(float left, float right) {
	auto result = left + right;
	if (!Value::FloatIsValid(result)) {
		throw OutOfRangeException("Overflow in addition of float!");
	}
	return result;
}

template <>
double AddOperator::Operation(double left, double right) {
	auto result = left + right;
	if (!Value::DoubleIsValid(result)) {
		throw OutOfRangeException("Overflow in addition of double!");
	}
	return result;
}

template <>
interval_t AddOperator::Operation(interval_t left, interval_t right) {
	left.months = AddOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(left.months, right.months);
	left.days = AddOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(left.days, right.days);
	left.micros = AddOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(left.micros, right.micros);
	return left;
}

template <>
date_t AddOperator::Operation(date_t left, interval_t right) {
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
		day = MinValue<int32_t>(day, Date::MonthDays(year, month));
		result = Date::FromDate(year, month, day);
	} else {
		result = left;
	}
	if (right.days != 0) {
		result += right.days;
	}
	if (right.micros != 0) {
		result += right.micros / Interval::MICROS_PER_DAY;
	}
	return result;
}

template <>
date_t AddOperator::Operation(interval_t left, date_t right) {
	return AddOperator::Operation<date_t, interval_t, date_t>(right, left);
}

dtime_t AddIntervalToTimeOperation(dtime_t left, interval_t right, date_t &date) {
	int64_t diff = right.micros - ((right.micros / Interval::MICROS_PER_DAY) * Interval::MICROS_PER_DAY);
	left += diff;
	if (left.micros >= Interval::MICROS_PER_DAY) {
		left.micros -= Interval::MICROS_PER_DAY;
		date.days++;
	} else if (left.micros < 0) {
		left.micros += Interval::MICROS_PER_DAY;
		date.days--;
	}
	return left;
}

template <>
timestamp_t AddOperator::Operation(timestamp_t left, interval_t right) {
	date_t date;
	dtime_t time;
	Timestamp::Convert(left, date, time);
	auto new_date = AddOperator::Operation<date_t, interval_t, date_t>(date, right);
	auto new_time = AddIntervalToTimeOperation(time, right, new_date);
	return Timestamp::FromDatetime(new_date, new_time);
}

template <>
timestamp_t AddOperator::Operation(interval_t left, timestamp_t right) {
	return AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(right, left);
}

//===--------------------------------------------------------------------===//
// + [add] with overflow check
//===--------------------------------------------------------------------===//
struct OverflowCheckedAddition {
	template <class SRCTYPE, class UTYPE>
	static inline bool Operation(SRCTYPE left, SRCTYPE right, SRCTYPE &result) {
		UTYPE uresult = AddOperator::Operation<UTYPE, UTYPE, UTYPE>(UTYPE(left), UTYPE(right));
		if (uresult < NumericLimits<SRCTYPE>::Minimum() || uresult > NumericLimits<SRCTYPE>::Maximum()) {
			return false;
		}
		result = SRCTYPE(uresult);
		return true;
	}
};

template <>
bool TryAddOperator::Operation(uint8_t left, uint8_t right, uint8_t &result) {
	return OverflowCheckedAddition::Operation<uint8_t, uint16_t>(left, right, result);
}
template <>
bool TryAddOperator::Operation(uint16_t left, uint16_t right, uint16_t &result) {
	return OverflowCheckedAddition::Operation<uint16_t, uint32_t>(left, right, result);
}
template <>
bool TryAddOperator::Operation(uint32_t left, uint32_t right, uint32_t &result) {
	return OverflowCheckedAddition::Operation<uint32_t, uint64_t>(left, right, result);
}

template <>
bool TryAddOperator::Operation(uint64_t left, uint64_t right, uint64_t &result) {
	if (NumericLimits<uint64_t>::Maximum() - left < right) {
		return false;
	}
	return OverflowCheckedAddition::Operation<uint64_t, uint64_t>(left, right, result);
}

template <>
bool TryAddOperator::Operation(int8_t left, int8_t right, int8_t &result) {
	return OverflowCheckedAddition::Operation<int8_t, int16_t>(left, right, result);
}

template <>
bool TryAddOperator::Operation(int16_t left, int16_t right, int16_t &result) {
	return OverflowCheckedAddition::Operation<int16_t, int32_t>(left, right, result);
}

template <>
bool TryAddOperator::Operation(int32_t left, int32_t right, int32_t &result) {
	return OverflowCheckedAddition::Operation<int32_t, int64_t>(left, right, result);
}

template <>
bool TryAddOperator::Operation(int64_t left, int64_t right, int64_t &result) {
#if (__GNUC__ >= 5) || defined(__clang__)
	if (__builtin_add_overflow(left, right, &result)) {
		return false;
	}
#else
	// https://blog.regehr.org/archives/1139
	result = int64_t((uint64_t)left + (uint64_t)right);
	if ((left < 0 && right < 0 && result >= 0) || (left >= 0 && right >= 0 && result < 0)) {
		return false;
	}
#endif
	return true;
}

//===--------------------------------------------------------------------===//
// add decimal with overflow check
//===--------------------------------------------------------------------===//
template <class T, T min, T max>
bool TryDecimalAddTemplated(T left, T right, T &result) {
	if (right < 0) {
		if (min - right > left) {
			return false;
		}
	} else {
		if (max - right < left) {
			return false;
		}
	}
	result = left + right;
	return true;
}

template <>
bool TryDecimalAdd::Operation(int16_t left, int16_t right, int16_t &result) {
	return TryDecimalAddTemplated<int16_t, -9999, 9999>(left, right, result);
}

template <>
bool TryDecimalAdd::Operation(int32_t left, int32_t right, int32_t &result) {
	return TryDecimalAddTemplated<int32_t, -999999999, 999999999>(left, right, result);
}

template <>
bool TryDecimalAdd::Operation(int64_t left, int64_t right, int64_t &result) {
	return TryDecimalAddTemplated<int64_t, -999999999999999999, 999999999999999999>(left, right, result);
}

template <>
bool TryDecimalAdd::Operation(hugeint_t left, hugeint_t right, hugeint_t &result) {
	result = left + right;
	if (result <= -Hugeint::POWERS_OF_TEN[38] || result >= Hugeint::POWERS_OF_TEN[38]) {
		return false;
	}
	return true;
}

template <>
hugeint_t DecimalAddOverflowCheck::Operation(hugeint_t left, hugeint_t right) {
	hugeint_t result;
	if (!TryDecimalAdd::Operation(left, right, result)) {
		throw OutOfRangeException("Overflow in addition of DECIMAL(38) (%s + %s);", left.ToString(), right.ToString());
	}
	return result;
}

//===--------------------------------------------------------------------===//
// add time operator
//===--------------------------------------------------------------------===//
template <>
dtime_t AddTimeOperator::Operation(dtime_t left, interval_t right) {
	date_t date(0);
	return AddIntervalToTimeOperation(left, right, date);
}

template <>
dtime_t AddTimeOperator::Operation(interval_t left, dtime_t right) {
	return AddTimeOperator::Operation<dtime_t, interval_t, dtime_t>(right, left);
}

} // namespace duckdb
