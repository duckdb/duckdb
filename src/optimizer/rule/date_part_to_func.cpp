#include "duckdb/optimizer/rule/date_trunc_simplification.hpp"

#include "duckdb/common/enums/date_part_specifier.hpp"

namespace duckdb {

//! The interval constructor for a date part. Separate from the rule: the parser also needs it, to read INTERVAL
//! literals.
string DateTruncSimplificationRule::DatePartToFunc(const DatePartSpecifier &date_part) {
	switch (date_part) {
	// These specifiers can be used as intervals.
	case DatePartSpecifier::YEAR:
		return "to_years";
	case DatePartSpecifier::MONTH:
		return "to_months";
	case DatePartSpecifier::DAY:
		return "to_days";
	case DatePartSpecifier::DECADE:
		return "to_decades";
	case DatePartSpecifier::CENTURY:
		return "to_centuries";
	case DatePartSpecifier::MILLENNIUM:
		return "to_millennia";
	case DatePartSpecifier::MICROSECONDS:
		return "to_microseconds";
	case DatePartSpecifier::MILLISECONDS:
		return "to_milliseconds";
	case DatePartSpecifier::SECOND:
		return "to_seconds";
	case DatePartSpecifier::MINUTE:
		return "to_minutes";
	case DatePartSpecifier::HOUR:
		return "to_hours";
	case DatePartSpecifier::WEEK:
		return "to_weeks";
	case DatePartSpecifier::QUARTER:
		return "to_quarters";

	// These specifiers cannot be used as intervals and can only be used as
	// date parts.
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::ISOYEAR:
	case DatePartSpecifier::YEARWEEK:
	case DatePartSpecifier::ERA:
	case DatePartSpecifier::TIMEZONE:
	case DatePartSpecifier::TIMEZONE_HOUR:
	case DatePartSpecifier::TIMEZONE_MINUTE:
	default:
		return "";
	}
}

} // namespace duckdb
