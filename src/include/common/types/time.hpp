//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/types/time.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

namespace duckdb {

//! The Date class is a static class that holds helper functions for the Time
//! type.
class Time {
public:
	//! Convert a string in the format "hh:mm:ss" to a time object
	static dtime_t FromString(string str);
	//! Convert a time object to a string in the format "hh:mm:ss"
	static string ToString(dtime_t time);

	static string Format(int32_t hour, int32_t minute, int32_t second);

	static dtime_t FromTime(int32_t hour, int32_t minute, int32_t second);

	static bool IsValidTime(int32_t hour, int32_t minute, int32_t second);
};
} // namespace duckdb
