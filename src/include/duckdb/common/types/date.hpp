//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/date.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! The Date class is a static class that holds helper functions for the Date
//! type.
class Date {
public:
	//! Convert a string in the format "YYYY-MM-DD" to a date object
	static date_t FromString(string str);
	//! Convert a string in the format "YYYY-MM-DD" to a date object
	static date_t FromCString(const char *str);
	//! Convert a date object to a string in the format "YYYY-MM-DD"
	static string ToString(date_t date);

	//! Create a string "YYYY-MM-DD" from a specified (year, month, day)
	//! combination
	static string Format(int32_t year, int32_t month, int32_t day);

	//! Extract the year, month and day from a given date object
	static void Convert(date_t date, int32_t &out_year, int32_t &out_month, int32_t &out_day);
	//! Create a Date object from a specified (year, month, day) combination
	static date_t FromDate(int32_t year, int32_t month, int32_t day);

	//! Returns true if (year) is a leap year, and false otherwise
	static bool IsLeapYear(int32_t year);

	//! Returns true if the specified (year, month, day) combination is a valid
	//! date
	static bool IsValidDay(int32_t year, int32_t month, int32_t day);

	//! Extract the epoch from the date (seconds since 1970-01-01)
	static int64_t Epoch(date_t date);
	//! Convert the epoch (seconds since 1970-01-01) to a date_t
	static date_t EpochToDate(int64_t epoch);
	//! Extract year of a date entry
	static int32_t ExtractYear(date_t date);
	//! Extract month of a date entry
	static int32_t ExtractMonth(date_t date);
	//! Extract day of a date entry
	static int32_t ExtractDay(date_t date);
	//! Extract the day of the week (1-7)
	static int32_t ExtractISODayOfTheWeek(date_t date);
	//! Extract the day of the year
	static int32_t ExtractDayOfTheYear(date_t date);
	//! Extract the week number
	static int32_t ExtractWeekNumber(date_t date);
	//! Returns the date of the monday of the current week.
	static date_t GetMondayOfCurrentWeek(date_t date);
};
} // namespace duckdb
