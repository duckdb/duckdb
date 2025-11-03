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

inline string DatePartSpecifierToString(DatePartSpecifier date_part) {
	switch (date_part) {
	case DatePartSpecifier::YEAR:
		return "YEAR";
	case DatePartSpecifier::MONTH:
		return "MONTH";
	case DatePartSpecifier::DAY:
		return "DAY";
	case DatePartSpecifier::DECADE:
		return "DECADE";
	case DatePartSpecifier::CENTURY:
		return "CENTURY";
	case DatePartSpecifier::MILLENNIUM:
		return "MILLENNIUM";
	case DatePartSpecifier::MICROSECONDS:
		return "MICROSECONDS";
	case DatePartSpecifier::MILLISECONDS:
		return "MILLISECONDS";
	case DatePartSpecifier::SECOND:
		return "SECOND";
	case DatePartSpecifier::MINUTE:
		return "MINUTE";
	case DatePartSpecifier::HOUR:
		return "HOUR";
	case DatePartSpecifier::DOW:
		return "DOW";
	case DatePartSpecifier::ISODOW:
		return "ISODOW";
	case DatePartSpecifier::WEEK:
		return "WEEK";
	case DatePartSpecifier::ISOYEAR:
		return "ISOYEAR";
	case DatePartSpecifier::QUARTER:
		return "QUARTER";
	case DatePartSpecifier::DOY:
		return "DOY";
	case DatePartSpecifier::YEARWEEK:
		return "YEARWEEK";
	case DatePartSpecifier::ERA:
		return "ERA";
	case DatePartSpecifier::TIMEZONE:
		return "TIMEZONE";
	case DatePartSpecifier::TIMEZONE_HOUR:
		return "TIMEZONE_HOUR";
	case DatePartSpecifier::TIMEZONE_MINUTE:
		return "TIMEZONE_MINUTE";
	case DatePartSpecifier::EPOCH:
		return "EPOCH";
	case DatePartSpecifier::JULIAN_DAY:
		return "JULIAN_DAY";
	case DatePartSpecifier::INVALID:
		return "INVALID";
	default:
		throw NotImplementedException("Unrecognized DatePartSpecifier");
	}
}

inline bool IsBigintDatepart(DatePartSpecifier part_code) {
	return size_t(part_code) < size_t(DatePartSpecifier::BEGIN_DOUBLE);
}

DUCKDB_API bool TryGetDatePartSpecifier(const string &specifier, DatePartSpecifier &result);
DUCKDB_API DatePartSpecifier GetDatePartSpecifier(const string &specifier);

} // namespace duckdb
