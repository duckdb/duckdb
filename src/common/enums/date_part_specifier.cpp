#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

namespace duckdb {

bool TryGetDatePartSpecifier(const string &specifier_p, DatePartSpecifier &result) {
	auto specifier = StringUtil::Lower(specifier_p);
	if (specifier == "year" || specifier == "yr" || specifier == "y" || specifier == "years" || specifier == "yrs") {
		result = DatePartSpecifier::YEAR;
	} else if (specifier == "month" || specifier == "mon" || specifier == "months" || specifier == "mons") {
		result = DatePartSpecifier::MONTH;
	} else if (specifier == "day" || specifier == "days" || specifier == "d" || specifier == "dayofmonth") {
		result = DatePartSpecifier::DAY;
	} else if (specifier == "decade" || specifier == "dec" || specifier == "decades" || specifier == "decs") {
		result = DatePartSpecifier::DECADE;
	} else if (specifier == "century" || specifier == "cent" || specifier == "centuries" || specifier == "c") {
		result = DatePartSpecifier::CENTURY;
	} else if (specifier == "millennium" || specifier == "mil" || specifier == "millenniums" ||
	           specifier == "millennia" || specifier == "mils" || specifier == "millenium") {
		result = DatePartSpecifier::MILLENNIUM;
	} else if (specifier == "microseconds" || specifier == "microsecond" || specifier == "us" || specifier == "usec" ||
	           specifier == "usecs" || specifier == "usecond" || specifier == "useconds") {
		result = DatePartSpecifier::MICROSECONDS;
	} else if (specifier == "milliseconds" || specifier == "millisecond" || specifier == "ms" || specifier == "msec" ||
	           specifier == "msecs" || specifier == "msecond" || specifier == "mseconds") {
		result = DatePartSpecifier::MILLISECONDS;
	} else if (specifier == "second" || specifier == "sec" || specifier == "seconds" || specifier == "secs" ||
	           specifier == "s") {
		result = DatePartSpecifier::SECOND;
	} else if (specifier == "minute" || specifier == "min" || specifier == "minutes" || specifier == "mins" ||
	           specifier == "m") {
		result = DatePartSpecifier::MINUTE;
	} else if (specifier == "hour" || specifier == "hr" || specifier == "hours" || specifier == "hrs" ||
	           specifier == "h") {
		result = DatePartSpecifier::HOUR;
	} else if (specifier == "epoch") {
		// seconds since 1970-01-01
		result = DatePartSpecifier::EPOCH;
	} else if (specifier == "dow" || specifier == "dayofweek" || specifier == "weekday") {
		// day of the week (Sunday = 0, Saturday = 6)
		result = DatePartSpecifier::DOW;
	} else if (specifier == "isodow") {
		// isodow (Monday = 1, Sunday = 7)
		result = DatePartSpecifier::ISODOW;
	} else if (specifier == "week" || specifier == "weeks" || specifier == "w" || specifier == "weekofyear") {
		// ISO week number
		result = DatePartSpecifier::WEEK;
	} else if (specifier == "doy" || specifier == "dayofyear") {
		// day of the year (1-365/366)
		result = DatePartSpecifier::DOY;
	} else if (specifier == "quarter" || specifier == "quarters") {
		// quarter of the year (1-4)
		result = DatePartSpecifier::QUARTER;
	} else if (specifier == "yearweek") {
		// Combined isoyear and isoweek YYYYWW
		result = DatePartSpecifier::YEARWEEK;
	} else if (specifier == "isoyear") {
		// ISO year (first week of the year may be in previous year)
		result = DatePartSpecifier::ISOYEAR;
	} else if (specifier == "era") {
		result = DatePartSpecifier::ERA;
	} else if (specifier == "timezone") {
		result = DatePartSpecifier::TIMEZONE;
	} else if (specifier == "timezone_hour") {
		result = DatePartSpecifier::TIMEZONE_HOUR;
	} else if (specifier == "timezone_minute") {
		result = DatePartSpecifier::TIMEZONE_MINUTE;
	} else if (specifier == "julian" || specifier == "jd") {
		result = DatePartSpecifier::JULIAN_DAY;
	} else {
		return false;
	}
	return true;
}

DatePartSpecifier GetDatePartSpecifier(const string &specifier) {
	DatePartSpecifier result;
	if (!TryGetDatePartSpecifier(specifier, result)) {
		throw ConversionException("extract specifier \"%s\" not recognized", specifier);
	}
	return result;
}

} // namespace duckdb
