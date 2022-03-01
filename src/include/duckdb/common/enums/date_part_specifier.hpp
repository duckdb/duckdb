//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/date_part_specifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class DatePartSpecifier : uint8_t {
	YEAR,
	MONTH,
	DAY,
	DECADE,
	CENTURY,
	MILLENNIUM,
	MICROSECONDS,
	MILLISECONDS,
	SECOND,
	MINUTE,
	HOUR,
	EPOCH,
	DOW,
	ISODOW,
	WEEK,
	ISOYEAR,
	QUARTER,
	DOY,
	YEARWEEK,
	ERA,
	TIMEZONE,
	TIMEZONE_HOUR,
	TIMEZONE_MINUTE
};

DUCKDB_API bool TryGetDatePartSpecifier(const string &specifier, DatePartSpecifier &result);
DUCKDB_API DatePartSpecifier GetDatePartSpecifier(const string &specifier);

} // namespace duckdb
