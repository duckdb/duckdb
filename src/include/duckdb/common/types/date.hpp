//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/date.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

//! The Date class is a static class that holds helper functions for the Date type.
class Date {
public:
	static const string_t MONTH_NAMES[12];
	static const string_t MONTH_NAMES_ABBREVIATED[12];
	static const string_t DAY_NAMES[7];
	static const string_t DAY_NAMES_ABBREVIATED[7];
	static const int32_t NORMAL_DAYS[13];
	static const int32_t CUMULATIVE_DAYS[13];
	static const int32_t LEAP_DAYS[13];
	static const int32_t CUMULATIVE_LEAP_DAYS[13];
	static const int32_t CUMULATIVE_YEAR_DAYS[401];
	static const int8_t MONTH_PER_DAY_OF_YEAR[365];
	static const int8_t LEAP_MONTH_PER_DAY_OF_YEAR[366];

	// min date is 5877642-06-25 (BC) (-2^31+2)
	constexpr static const int32_t DATE_MIN_YEAR = -5877641;
	constexpr static const int32_t DATE_MIN_MONTH = 6;
	constexpr static const int32_t DATE_MIN_DAY = 25;
	// max date is 5881580-07-10 (2^31-2)
	constexpr static const int32_t DATE_MAX_YEAR = 5881580;
	constexpr static const int32_t DATE_MAX_MONTH = 7;
	constexpr static const int32_t DATE_MAX_DAY = 10;
	constexpr static const int32_t EPOCH_YEAR = 1970;

	constexpr static const int32_t YEAR_INTERVAL = 400;
	constexpr static const int32_t DAYS_PER_YEAR_INTERVAL = 146097;

public:
	//! Convert a string in the format "YYYY-MM-DD" to a date object
	DUCKDB_API static date_t FromString(const string &str, bool strict = false);
	//! Convert a string in the format "YYYY-MM-DD" to a date object
	DUCKDB_API static date_t FromCString(const char *str, idx_t len, bool strict = false);
	//! Convert a date object to a string in the format "YYYY-MM-DD"
	DUCKDB_API static string ToString(date_t date);
	//! Try to convert text in a buffer to a date; returns true if parsing was successful
	DUCKDB_API static bool TryConvertDate(const char *buf, idx_t len, idx_t &pos, date_t &result, bool strict = false);

	//! Create a string "YYYY-MM-DD" from a specified (year, month, day)
	//! combination
	DUCKDB_API static string Format(int32_t year, int32_t month, int32_t day);

	//! Extract the year, month and day from a given date object
	DUCKDB_API static void Convert(date_t date, int32_t &out_year, int32_t &out_month, int32_t &out_day);
	//! Create a Date object from a specified (year, month, day) combination
	DUCKDB_API static date_t FromDate(int32_t year, int32_t month, int32_t day);
	DUCKDB_API static bool TryFromDate(int32_t year, int32_t month, int32_t day, date_t &result);

	//! Returns true if (year) is a leap year, and false otherwise
	DUCKDB_API static bool IsLeapYear(int32_t year);

	//! Returns true if the specified (year, month, day) combination is a valid
	//! date
	DUCKDB_API static bool IsValid(int32_t year, int32_t month, int32_t day);

	//! Returns true if the specified date is finite
	static inline bool IsFinite(date_t date) {
		return date != date_t::infinity() && date != date_t::ninfinity();
	}

	//! The max number of days in a month of a given year
	DUCKDB_API static int32_t MonthDays(int32_t year, int32_t month);

	//! Extract the epoch from the date (seconds since 1970-01-01)
	DUCKDB_API static int64_t Epoch(date_t date);
	//! Extract the epoch from the date (nanoseconds since 1970-01-01)
	DUCKDB_API static int64_t EpochNanoseconds(date_t date);
	//! Convert the epoch (seconds since 1970-01-01) to a date_t
	DUCKDB_API static date_t EpochToDate(int64_t epoch);

	//! Extract the number of days since epoch (days since 1970-01-01)
	DUCKDB_API static int32_t EpochDays(date_t date);
	//! Convert the epoch number of days to a date_t
	DUCKDB_API static date_t EpochDaysToDate(int32_t epoch);

	//! Extract year of a date entry
	DUCKDB_API static int32_t ExtractYear(date_t date);
	//! Extract year of a date entry, but optimized to first try the last year found
	DUCKDB_API static int32_t ExtractYear(date_t date, int32_t *last_year);
	DUCKDB_API static int32_t ExtractYear(timestamp_t ts, int32_t *last_year);
	//! Extract month of a date entry
	DUCKDB_API static int32_t ExtractMonth(date_t date);
	//! Extract day of a date entry
	DUCKDB_API static int32_t ExtractDay(date_t date);
	//! Extract the day of the week (1-7)
	DUCKDB_API static int32_t ExtractISODayOfTheWeek(date_t date);
	//! Extract the day of the year
	DUCKDB_API static int32_t ExtractDayOfTheYear(date_t date);
	//! Extract the ISO week number
	//! ISO weeks start on Monday and the first week of a year
	//! contains January 4 of that year.
	//! In the ISO week-numbering system, it is possible for early-January dates
	//! to be part of the 52nd or 53rd week of the previous year.
	DUCKDB_API static void ExtractISOYearWeek(date_t date, int32_t &year, int32_t &week);
	DUCKDB_API static int32_t ExtractISOWeekNumber(date_t date);
	DUCKDB_API static int32_t ExtractISOYearNumber(date_t date);
	//! Extract the week number as Python handles it.
	//! Either Monday or Sunday is the first day of the week,
	//! and any date before the first Monday/Sunday returns week 0
	//! This is a bit more consistent because week numbers in a year are always incrementing
	DUCKDB_API static int32_t ExtractWeekNumberRegular(date_t date, bool monday_first = true);
	//! Returns the date of the monday of the current week.
	DUCKDB_API static date_t GetMondayOfCurrentWeek(date_t date);

	//! Helper function to parse two digits from a string (e.g. "30" -> 30, "03" -> 3, "3" -> 3)
	DUCKDB_API static bool ParseDoubleDigit(const char *buf, idx_t len, idx_t &pos, int32_t &result);

	DUCKDB_API static string ConversionError(const string &str);
	DUCKDB_API static string ConversionError(string_t str);

private:
	static void ExtractYearOffset(int32_t &n, int32_t &year, int32_t &year_offset);
};
} // namespace duckdb
