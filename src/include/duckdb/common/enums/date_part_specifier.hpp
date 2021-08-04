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
	QUARTER,
	DOY
};

bool TryGetDatePartSpecifier(const string &specifier, DatePartSpecifier &result);
DatePartSpecifier GetDatePartSpecifier(const string &specifier);

} // namespace duckdb
