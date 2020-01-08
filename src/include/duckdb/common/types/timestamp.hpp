//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/timestamp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

#include <chrono>  // chrono::system_clock
#include <ctime>   // localtime
#include <iomanip> // put_time
#include <sstream> // stringstream
#include <string>  // string

namespace duckdb {

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

	// Unix epoch: milliseconds since 1970
	static int64_t GetEpoch(timestamp_t timestamp);
	// Seconds including fractional part multiplied by 1000
	static int64_t GetMilliseconds(timestamp_t timestamp);
	static int64_t GetSeconds(timestamp_t timestamp);
	static int64_t GetMinutes(timestamp_t timestamp);
	static int64_t GetHours(timestamp_t timestamp);
};
} // namespace duckdb
