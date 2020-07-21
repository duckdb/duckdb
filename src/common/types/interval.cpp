#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/types/time.hpp"

using namespace std;

namespace duckdb {

constexpr const int32_t MONTHS_PER_MILLENIUM = 12000;
constexpr const int32_t MONTHS_PER_CENTURY = 1200;
constexpr const int32_t MONTHS_PER_DECADE = 120;
constexpr const int32_t MONTHS_PER_YEAR = 12;
constexpr const int32_t MONTHS_PER_QUARTER = 3;
constexpr const int32_t DAYS_PER_WEEK = 7;
constexpr const int64_t MSECS_PER_HOUR = 3600000;
constexpr const int64_t MSECS_PER_MINUTE = 60000;
constexpr const int64_t MSECS_PER_SEC = 1000;

bool Interval::FromString(string str, interval_t &result) {
	return Interval::FromCString(str.c_str(), str.size(), result);
}

bool Interval::FromCString(const char *str, idx_t len, interval_t &result) {
	idx_t pos = 0;
	idx_t start_pos;
	bool negative;
	bool found_any = false;
	int64_t number;
	DatePartSpecifier specifier;

	result.days = 0;
	result.msecs = 0;
	result.months = 0;

	switch(str[pos]) {
	case '@':
		pos++;
		goto standard_interval;
	case 'P':
	case 'p':
		pos++;
		goto posix_interval;
	default:
		goto standard_interval;
	}
standard_interval:
	// start parsing a standard interval (e.g. 2 years 3 months...)
	for(; pos < len; pos++) {
		char c = str[pos];
		if (c == ' ' || c == '\t' || c == '\n') {
			// skip spaces
			continue;
		} else if (c >= '0' && c <= '9') {
			// start parsing a positive number
			negative = false;
			goto parse_number;
		} else if (c == '-') {
			// negative number
			negative = true;
			pos++;
			goto parse_number;
		} else if (c == 'a' or c == 'A') {
			// parse the word "ago" as the final specifier
			goto parse_ago;
		} else {
			// unrecognized character, expected a number or end of string
			return false;
		}
	}
	goto end_of_string;
parse_number:
	start_pos = pos;
	for(; pos < len; pos++) {
		char c = str[pos];
		if (c >= '0' && c <= '9') {
			// the number continues
			continue;
		} else {
			if (pos == start_pos) {
				return false;
			}
			// finished the number, parse it from the string
			string_t nr_string(str + start_pos, pos - start_pos);
			number = Cast::Operation<string_t, int64_t>(nr_string);
			if (negative) {
				number = -number;
			}
			goto parse_identifier;
		}
	}
	goto end_of_string;
parse_identifier:
	for(; pos < len; pos++) {
		char c = str[pos];
		if (c == ' ' || c == '\t' || c == '\n') {
			// skip spaces at the start
			continue;
		} else {
			break;
		}
	}
	// now parse the identifier
	start_pos = pos;
	for(; pos < len; pos++) {
		char c = str[pos];
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			// keep parsing the string
			continue;
		} else {
			break;
		}
	}
	specifier = GetDatePartSpecifier(string(str + start_pos, pos - start_pos));
	// add the specifier to the interval
	switch(specifier) {
	case DatePartSpecifier::MILLENNIUM:
		result.months += number * MONTHS_PER_MILLENIUM;
		break;
	case DatePartSpecifier::CENTURY:
		result.months += number * MONTHS_PER_CENTURY;
		break;
	case DatePartSpecifier::DECADE:
		result.months += number * MONTHS_PER_DECADE;
		break;
	case DatePartSpecifier::YEAR:
		result.months += number * MONTHS_PER_YEAR;
		break;
	case DatePartSpecifier::QUARTER:
		result.months += number * MONTHS_PER_QUARTER;
		break;
	case DatePartSpecifier::MONTH:
		result.months += number;
		break;
	case DatePartSpecifier::DAY:
		result.days += number;
		break;
	case DatePartSpecifier::MICROSECONDS:
		result.msecs += number / 1000;
		break;
	case DatePartSpecifier::MILLISECONDS:
		result.msecs += number;
		break;
	case DatePartSpecifier::SECOND:
		result.msecs += number * MSECS_PER_SEC;
		break;
	case DatePartSpecifier::MINUTE:
		result.msecs += number * MSECS_PER_MINUTE;
		break;
	case DatePartSpecifier::HOUR:
		result.msecs += number * MSECS_PER_HOUR;
		break;
	case DatePartSpecifier::WEEK:
		result.days += number * DAYS_PER_WEEK;
		break;
	default:
		return false;
	}
	found_any = true;
	goto standard_interval;
parse_ago:
	// parse the "ago" string at the end of the
	if (len - pos < 3) {
		return false;
	}
	if (!(str[pos] == 'a' || str[pos == 'A'])) {
		return false;
	}
	pos++;
	if (!(str[pos] == 'g' || str[pos == 'G'])) {
		return false;
	}
	pos++;
	if (!(str[pos] == 'o' || str[pos == 'O'])) {
		return false;
	}
	pos++;
	// parse any trailing whitespace
	for(; pos < len; pos++) {
		char c = str[pos];
		if (c == ' ' || c == '\t' || c == '\n') {
			continue;
		} else {
			return false;
		}
	}
	// invert all the values
	result.months = -result.months;
	result.days = -result.days;
	result.msecs = -result.msecs;
	goto end_of_string;
end_of_string:
	if (!found_any) {
		// end of string and no identifiers were found: cannot convert empty interval
		return false;
	}
	return true;
posix_interval:
	return false;
}

string Interval::ToString(interval_t date) {
	string result;
	if (date.months != 0) {
		int32_t years = date.months / 12;
		int32_t months = date.months - years * 12;
		if (years != 0) {
			result += to_string(years) + (years != 1 ? " years" : " year");
		}
		if (months != 0) {
			if (!result.empty()) {
				result += " ";
			}
			result += to_string(months) + (months != 1 ? " months" : " month");
		}
	}
	if (date.days != 0) {
		if (!result.empty()) {
			result += " ";
		}
		result += to_string(date.days) + (date.days != 1 ? " days" : " day");
	}
	if (date.msecs != 0) {
		if (!result.empty()) {
			result += " ";
		}
		int64_t msecs = date.msecs;
		if (msecs < 0) {
			result += "-";
			msecs = -msecs;
		}
		int64_t hours = msecs / MSECS_PER_HOUR;
		msecs -= hours * MSECS_PER_HOUR;
		int32_t minutes = msecs / MSECS_PER_MINUTE;
		msecs -= minutes * MSECS_PER_MINUTE;
		int32_t secs = msecs / MSECS_PER_SEC;
		msecs -= secs * MSECS_PER_SEC;
		if (hours < 10) {
			result += "0";
		}
		result += to_string(hours) + ":";
		if (minutes < 10) {
			result += "0";
		}
		result += to_string(minutes) + ":";
		if (secs < 10) {
			result += "0";
		}
		result += to_string(secs);
		if (msecs > 0) {
			result += ".";
			if (msecs < 100) {
				result += "0";
			}
			if (msecs < 10) {
				result += "0";
			}
			result += to_string(msecs);
		}
	}
	return result;
}


}
