//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/types/timestamp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

#include <chrono>  // chrono::system_clock
#include <ctime>   // localtime
#include <iomanip> // put_time
#include <sstream> // stringstream
#include <string>  // string

namespace duckdb {

constexpr const double DAYS_PER_YEAR = 365.25; //! assumes leap year every four years
constexpr const int32_t MONTHS_PER_YEAR = 12;
constexpr const int32_t DAYS_PER_MONTH = 30; //! assumes exactly 30 days per month (ISO 8601 suggest 30 days)
constexpr const int32_t HOURS_PER_DAY = 24;  //! assume no daylight savings time changes
constexpr const int32_t STD_TIMESTAMP_LENGTH = 19;
constexpr const int32_t MAX_TIMESTAMP_LENGTH = 23;
constexpr const int32_t START_YEAR = 1900;
constexpr const char *DEFAULT_TIME = "00:00:00";

/*
 *	This doesn't adjust for uneven daylight savings time intervals or leap
 *	seconds, and it crudely estimates leap years.  A more accurate value
 *	for days per years is 365.2422.
 */
constexpr const size_t SECS_PER_YEAR = (36525 * 864); /* avoid floating-point computation */
constexpr const int32_t SECS_PER_DAY = 86400;
constexpr const int32_t SECS_PER_HOUR = 3600;
constexpr const int32_t SECS_PER_MINUTE = 60;
constexpr const int32_t MINS_PER_H = 60;
constexpr const int64_t MSECS_PER_DAY = 8640000;
constexpr const int64_t MSECS_PER_HOUR = 360000;
constexpr const int64_t MSECS_PER_MINUTE = 60000;
constexpr const int64_t MSECS_PER_SEC = 1000;

// Used to check amount of days per month in common year and leap year
constexpr int days_per_month[2][13] = {{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0},
                                       {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0}};
constexpr bool isleap(int16_t year) {
	return (((year) % 4) == 0 && (((year) % 100) != 0 || ((year) % 400) == 0));
}

struct Interval {
	int64_t time;
	int32_t days;   //! days, after time for alignment
	int32_t months; //! months after time for alignment
};

struct timestamp_struct {
	int32_t year;
	int8_t month;
	int8_t day;
	int8_t hour;
	int8_t min;
	int8_t sec;
	int16_t msec;
};
//! The Timestamp class is a static class that holds helper functions for the Timestamp
//! type.
class Timestamp {
public:
	//! Convert a string in the format "YYYY-MM-DD hh:mm:ss" to a timestamp object
	static timestamp_t FromString(string str);
	//! Convert a date object to a string in the format "YYYY-MM-DDThh:mm:ssZ"
	static string ToString(timestamp_t timestamp);

	static date_t GetDate(timestamp_t timestamp);

	static dtime_t GetTime(timestamp_t timestamp);
	//! Create a Timestamp object from a specified (date, time) combination
	static timestamp_t FromDatetime(date_t date, dtime_t time);
	//! Extract the date and time from a given timestamp object
	static void Convert(timestamp_t date, date_t &out_date, dtime_t &out_time);
	//! Returns current timestamp
	static timestamp_t GetCurrentTimestamp();
	//! Gets the timestamp which correspondes to the difference between the given ones
	static Interval GetDifference(timestamp_t timestamp_a, timestamp_t timestamp_b);

	static timestamp_struct IntervalToTimestamp(Interval &interval);
};
} // namespace duckdb
