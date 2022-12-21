//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-dateadd.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "icu-datefunc.hpp"

namespace duckdb {

void RegisterICUDateAddFunctions(ClientContext &context);

struct ICUCalendarAdd {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right, icu::Calendar *calendar) {
		throw InternalException("Unimplemented type for ICUCalendarAdd");
	}
};

struct ICUCalendarSub : public ICUDateFunc {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right, icu::Calendar *calendar) {
		throw InternalException("Unimplemented type for ICUCalendarSub");
	}
};

template <>
timestamp_t ICUCalendarAdd::Operation(timestamp_t timestamp, interval_t interval, icu::Calendar *calendar) {
	int64_t millis = timestamp.value / Interval::MICROS_PER_MSEC;
	int64_t micros = timestamp.value % Interval::MICROS_PER_MSEC;

	// Manually move the µs
	micros += interval.micros % Interval::MICROS_PER_MSEC;
	if (micros >= Interval::MICROS_PER_MSEC) {
		micros -= Interval::MICROS_PER_MSEC;
		++millis;
	} else if (micros < 0) {
		micros += Interval::MICROS_PER_MSEC;
		--millis;
	}

	// Make sure the value is still in range
	date_t d;
	dtime_t t;
	auto us = MultiplyOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(millis, Interval::MICROS_PER_MSEC);
	Timestamp::Convert(timestamp_t(us), d, t);

	// Now use the calendar to add the other parts
	UErrorCode status = U_ZERO_ERROR;
	const auto udate = UDate(millis);
	calendar->setTime(udate, status);

	// Add interval fields from lowest to highest
	calendar->add(UCAL_MILLISECOND, interval.micros / Interval::MICROS_PER_MSEC, status);
	calendar->add(UCAL_DATE, interval.days, status);
	calendar->add(UCAL_MONTH, interval.months, status);

	return ICUDateFunc::GetTime(calendar, micros);
}

template <>
timestamp_t ICUCalendarAdd::Operation(interval_t interval, timestamp_t timestamp, icu::Calendar *calendar) {
	return Operation<timestamp_t, interval_t, timestamp_t>(timestamp, interval, calendar);
}

template <>
timestamp_t ICUCalendarSub::Operation(timestamp_t timestamp, interval_t interval, icu::Calendar *calendar) {
	const interval_t negated {-interval.months, -interval.days, -interval.micros};
	return ICUCalendarAdd::template Operation<timestamp_t, interval_t, timestamp_t>(timestamp, negated, calendar);
}

template <>
interval_t ICUCalendarSub::Operation(timestamp_t end_date, timestamp_t start_date, icu::Calendar *calendar) {
	if (start_date > end_date) {
		auto negated = Operation<timestamp_t, timestamp_t, interval_t>(start_date, end_date, calendar);
		return {-negated.months, -negated.days, -negated.micros};
	}

	auto start_micros = ICUDateFunc::SetTime(calendar, start_date);
	auto end_micros = (uint64_t)(end_date.value % Interval::MICROS_PER_MSEC);

	// Borrow 1ms from end_date if we wrap. This works because start_date <= end_date
	// and if the µs are out of order, then there must be an extra ms.
	if (start_micros > (idx_t)end_micros) {
		end_date.value -= Interval::MICROS_PER_MSEC;
		end_micros += Interval::MICROS_PER_MSEC;
	}

	//	Timestamp differences do not use months, so start with days
	interval_t result;
	result.months = 0;
	result.days = SubtractField(calendar, UCAL_DATE, end_date);

	auto hour_diff = SubtractField(calendar, UCAL_HOUR_OF_DAY, end_date);
	auto min_diff = SubtractField(calendar, UCAL_MINUTE, end_date);
	auto sec_diff = SubtractField(calendar, UCAL_SECOND, end_date);
	auto ms_diff = SubtractField(calendar, UCAL_MILLISECOND, end_date);
	auto micros_diff = ms_diff * Interval::MICROS_PER_MSEC + (end_micros - start_micros);
	result.micros = Time::FromTime(hour_diff, min_diff, sec_diff, micros_diff).micros;

	return result;
}

} // namespace duckdb
