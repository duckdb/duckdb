//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/interval.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

//! The Interval class is a static class that holds helper functions for the Interval
//! type.
class Interval {
public:
	static constexpr const int32_t MONTHS_PER_MILLENIUM = 12000;
	static constexpr const int32_t MONTHS_PER_CENTURY = 1200;
	static constexpr const int32_t MONTHS_PER_DECADE = 120;
	static constexpr const int32_t MONTHS_PER_YEAR = 12;
	static constexpr const int32_t MONTHS_PER_QUARTER = 3;
	static constexpr const int32_t DAYS_PER_WEEK = 7;
	static constexpr const int64_t MSECS_PER_DAY = 86400000;
	static constexpr const int64_t MSECS_PER_HOUR = 3600000;
	static constexpr const int64_t MSECS_PER_MINUTE = 60000;
	static constexpr const int64_t MSECS_PER_SEC = 1000;
	static constexpr const int32_t SECS_PER_MINUTE = 60;
	static constexpr const int32_t MINS_PER_HOUR = 60;
	static constexpr const int32_t HOURS_PER_DAY = 24;

public:
	//! Convert a string to an interval object
	static bool FromString(string str, interval_t &result);
	//! Convert a string to an interval object
	static bool FromCString(const char *str, idx_t len, interval_t &result);
	//! Convert an interval object to a string
	static string ToString(interval_t date);

	//! Returns the difference between two timestamps
	static interval_t GetDifference(timestamp_t timestamp_1, timestamp_t timestamp_2);
};
} // namespace duckdb
