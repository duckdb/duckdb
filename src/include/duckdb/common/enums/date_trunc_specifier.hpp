//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/date_trunc_specifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class DateTruncSpecifier : uint8_t {
	MICROSECONDS,
	MILLISECONDS,
	SECOND,
	MINUTE,
	HOUR,
	DAY,
	WEEK,
	MONTH,
	QUARTER,
	YEAR,
	DECADE,
	CENTURY,
	MILLENIUM,
};

DateTruncSpecifier GetDateTruncSpecifier(string specifier);

} // namespace duckdb
