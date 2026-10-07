//===----------------------------------------------------------------------===//
//                         DuckDB
//
// grego.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/limits.hpp"

#include <cmath>

namespace duckdb {
namespace datetime {

//! Milliseconds per unit of time
static constexpr int64_t MILLIS_PER_SECOND = 1000;
static constexpr int64_t MILLIS_PER_MINUTE = 60 * MILLIS_PER_SECOND;
static constexpr int64_t MILLIS_PER_HOUR = 60 * MILLIS_PER_MINUTE;
static constexpr int64_t MILLIS_PER_DAY = 24 * MILLIS_PER_HOUR;
static constexpr int64_t MILLIS_PER_WEEK = 7 * MILLIS_PER_DAY;

//! January 1, 1 CE in the proleptic Gregorian calendar, as a Julian day
static constexpr int32_t JULIAN_1_CE = 1721426;
//! January 1, 1970 CE in the proleptic Gregorian calendar, as a Julian day
static constexpr int32_t JULIAN_1970_CE = 2440588;

//! The year that the epoch starts in, which fields default to
static constexpr int32_t EPOCH_YEAR = 1970;

//! The supported Julian day range. Values outside of it cannot be represented as milliseconds.
static constexpr int32_t MIN_JULIAN = -0x7F000000;
static constexpr int32_t MAX_JULIAN = +0x7F000000;

//! Division that rounds towards negative infinity, so that the remainder is never negative
struct FloorDiv {
	static int32_t Divide(int32_t numerator, int32_t denominator) {
		return (numerator >= 0) ? numerator / denominator : ((numerator + 1) / denominator) - 1;
	}
	static int64_t Divide(int64_t numerator, int64_t denominator) {
		return (numerator >= 0) ? numerator / denominator : ((numerator + 1) / denominator) - 1;
	}
	static double Divide(double numerator, double denominator) {
		return std::floor(numerator / denominator);
	}
	static int32_t Divide(int32_t numerator, int32_t denominator, int32_t &remainder) {
		const auto quotient = Divide(numerator, denominator);
		// the remainder is always smaller than the denominator, but the product it is taken
		// from is not, so it is formed wide enough to hold the extremes of the numerator
		remainder = int32_t(int64_t(numerator) - int64_t(quotient) * denominator);
		return quotient;
	}
	//! Divides a millisecond count by a whole number of milliseconds, keeping the remainder exact
	static int64_t Divide(int64_t numerator, int32_t denominator, int32_t &remainder) {
		const auto quotient = Divide(numerator, int64_t(denominator));
		remainder = static_cast<int32_t>(numerator - quotient * denominator);
		return quotient;
	}
};

//! Proleptic Gregorian calendar arithmetic shared by the time zone and calendar code.
//! Years are extended years, i.e. 0 is 1 BCE, -1 is 2 BCE, and so on.
struct Grego {
	static bool IsLeapYear(int32_t year) {
		return ((year & 0x3) == 0) && ((year % 100 != 0) || (year % 400 == 0));
	}
	//! The number of days in a 0-based month
	static int8_t MonthLength(int32_t month, bool is_leap) {
		return MONTH_LENGTH[month + (is_leap ? 12 : 0)];
	}
	static int8_t MonthLength(int32_t year, int32_t month) {
		return MonthLength(month, IsLeapYear(year));
	}
	//! The number of days in the month before a 0-based month
	static int8_t PreviousMonthLength(int32_t year, int32_t month) {
		return (month > 0) ? MonthLength(year, month - 1) : 31;
	}
	//! The number of days before a 0-based month in its year
	static int16_t DaysBeforeMonth(int32_t month, bool is_leap) {
		return DAYS_BEFORE[month + (is_leap ? 12 : 0)];
	}
	static int16_t DaysBeforeMonth(int32_t year, int32_t month) {
		return DaysBeforeMonth(month, IsLeapYear(year));
	}

	//! Converts a year, 0-based month and 1-based day of month to days since 1970-01-01
	static int64_t FieldsToDay(int32_t year, int32_t month, int32_t dom);
	//! Converts days since 1970-01-01 to a year and 1-based day of year
	static int32_t DayToYear(int32_t day, int16_t &doy);
	//! Converts days since 1970-01-01 to a year, 0-based month, 1-based day of month,
	//! 1-based day of week (1 == Sunday) and 1-based day of year
	static void DayToFields(int32_t day, int32_t &year, int8_t &month, int8_t &dom, int8_t &dow, int16_t &doy);
	//! Converts milliseconds since 1970-01-01 to date fields and the milliseconds within the day.
	//! Returns false if the time is outside of the supported range.
	static bool TimeToFields(int64_t time, int32_t &year, int8_t &month, int8_t &dom, int8_t &dow, int16_t &doy,
	                         int32_t &mid);
	//! The 1-based day of week (1 == Sunday) of a day since 1970-01-01
	static int32_t DayOfWeek(int32_t day);
	//! The 1-based day of week (1 == Sunday) of a Julian day, which is Monday on day zero
	static int32_t JulianDayToDayOfWeek(int32_t julian) {
		const auto dow = int32_t((int64_t(julian) + 1) % 7);
		return dow + (dow < 0 ? 8 : 1);
	}
	//! The ordinal of the day of week within its month: 1, 2, 3, 4 or -1 for the last one
	static int32_t DayOfWeekInMonth(int32_t year, int32_t month, int32_t dom);

	//! Converts a Julian day to milliseconds since 1970-01-01
	static int64_t JulianDayToMillis(int32_t julian) {
		return (static_cast<int64_t>(julian) - JULIAN_1970_CE) * static_cast<int64_t>(MILLIS_PER_DAY);
	}
	//! Converts milliseconds since 1970-01-01 to a Julian day
	static int32_t MillisToJulianDay(int64_t millis) {
		return static_cast<int32_t>(JULIAN_1970_CE + FloorDiv::Divide(millis, int64_t(MILLIS_PER_DAY)));
	}
	//! The number of days to add to a Julian calendar day to obtain the Gregorian calendar day
	static int32_t GregorianShift(int32_t eyear) {
		const int64_t y = static_cast<int64_t>(eyear) - 1;
		return static_cast<int32_t>(FloorDiv::Divide(y, int64_t(400)) - FloorDiv::Divide(y, int64_t(100)) + 2);
	}

private:
	static const int16_t DAYS_BEFORE[24];
	static const int8_t MONTH_LENGTH[24];
};

} // namespace datetime
} // namespace duckdb
