//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "unicode/timezone.h"

namespace duckdb {

struct TimestampData {
	int32_t date_units[3];
	int32_t time_units[4];
};

struct ICUHelpers {
	//! Tries to get a time zone - returns nullptr if the timezone is not found
	static unique_ptr<icu::TimeZone> TryGetTimeZone(string &tz_str);
	//! Gets a time zone - throws an error if the timezone is not found
	static unique_ptr<icu::TimeZone> GetTimeZone(string &tz_str);

	static TimestampData DecomposeTimestamp(timestamp_tz_t ts, icu::Calendar *calendar);

	static timestamp_t ToTimestamp(TimestampData data);
};

} // namespace duckdb
