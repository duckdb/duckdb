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

struct ICUHelpers {
	//! Tries to get a time zone - returns nullptr if the timezone is not found
	static unique_ptr<icu::TimeZone> TryGetTimeZone(string &tz_str);
	//! Gets a time zone - throws an error if the timezone is not found
	static unique_ptr<icu::TimeZone> GetTimeZone(string &tz_str);
};

} // namespace duckdb
