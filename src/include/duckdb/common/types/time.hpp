//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/time.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! The Date class is a static class that holds helper functions for the Time
//! type.
class Time {
public:
	//! Convert a string in the format "hh:mm:ss" to a time object
	static dtime_t FromString(string str);
	static dtime_t FromCString(const char *buf);

	//! Convert a time object to a string in the format "hh:mm:ss"
	static string ToString(dtime_t time);

	static string Format(int32_t hour, int32_t minute, int32_t second, int32_t milisecond = 0);

	static dtime_t FromTime(int32_t hour, int32_t minute, int32_t second, int32_t milisecond = 0);

	static bool IsValidTime(int32_t hour, int32_t minute, int32_t second, int32_t milisecond = 0);
	//! Extract the time from a given timestamp object
	static void Convert(dtime_t time, int32_t &out_hour, int32_t &out_min, int32_t &out_sec, int32_t &out_msec);
};
} // namespace duckdb
