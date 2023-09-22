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
	//	BIGINT values
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
	TIMEZONE_MINUTE,

	//	DOUBLE values
	EPOCH,
	JULIAN_DAY,

	//	Invalid
	INVALID,

	//	Type ranges
	BEGIN_BIGINT = YEAR,
	BEGIN_DOUBLE = EPOCH,
	BEGIN_INVALID = INVALID,
};

inline bool IsBigintDatepart(DatePartSpecifier part_code) {
	return size_t(part_code) < size_t(DatePartSpecifier::BEGIN_DOUBLE);
}

DUCKDB_API bool TryGetDatePartSpecifier(const string &specifier, DatePartSpecifier &result);
DUCKDB_API DatePartSpecifier GetDatePartSpecifier(const string &specifier);

} // namespace duckdb
