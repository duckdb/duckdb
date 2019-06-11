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
constexpr const size_t MONTHS_PER_YEAR = 12;
constexpr const size_t DAYS_PER_MONTH = 30; //! assumes exactly 30 days per month (ISO 8601 suggest 30 days)
constexpr const size_t HOURS_PER_DAY = 24;  //! assume no daylight savings time changes
constexpr const size_t STD_TIMESTAMP_LENGTH = 19;
constexpr const size_t MAX_TIMESTAMP_LENGTH = 23;
constexpr const char *DEFAULT_TIME = " 00:00:00";

struct Interval {
	int64_t time;
	int32_t day;   //! days, after time for alignment
	int32_t month; //! months after time for alignment
};

//! The Date class is a static class that holds helper functions for the Timestamp
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
	static timestamp_t GetDifference(timestamp_t timestamp_a, timestamp_t timestamp_b);
};
} // namespace duckdb
