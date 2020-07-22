#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"

using namespace std;

namespace duckdb {

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
	} else if (result.empty()) {
		return "00:00:00";
	}
	return result;
}

interval_t Interval::GetDifference(timestamp_t timestamp_1, timestamp_t timestamp_2) {
	// First extract the dates
	auto date1 = Timestamp::GetDate(timestamp_1);
	auto date2 = Timestamp::GetDate(timestamp_2);
	// and from date extract the years, months and days
	int32_t year1, month1, day1;
	int32_t year2, month2, day2;
	Date::Convert(date1, year1, month1, day1);
	Date::Convert(date2, year2, month2, day2);
	// finally perform the differences
	auto year_diff = year1 - year2;
	auto month_diff = month1 - month2;
	auto day_diff = day1 - day2;

	// Now we extract the time
	auto time1 = Timestamp::GetTime(timestamp_1);
	auto time2 = Timestamp::GetTime(timestamp_2);

	// In case time is not specified we do not show it in the output
	if (time1 == 0) {
		time2 = time1;
	}

	// and from time extract hours, minutes, seconds and miliseconds
	int32_t hour1, min1, sec1, msec1;
	int32_t hour2, min2, sec2, msec2;
	Time::Convert(time1, hour1, min1, sec1, msec1);
	Time::Convert(time2, hour2, min2, sec2, msec2);
	// finally perform the differences
	auto hour_diff = hour1 - hour2;
	auto min_diff = min1 - min2;
	auto sec_diff = sec1 - sec2;
	auto msec_diff = msec1 - msec2;

	// flip sign if necessary
	if (timestamp_1 < timestamp_2) {
		year_diff = -year_diff;
		month_diff = -month_diff;
		day_diff = -day_diff;
		hour_diff = -hour_diff;
		min_diff = -min_diff;
		sec_diff = -sec_diff;
		msec_diff = -msec_diff;
	}
	// now propagate any negative field into the next higher field
	while (msec_diff < 0) {
		msec_diff += MSECS_PER_SEC;
		sec_diff--;
	}
	while (sec_diff < 0) {
		sec_diff += SECS_PER_MINUTE;
		min_diff--;
	}
	while (min_diff < 0) {
		min_diff += MINS_PER_HOUR;
		hour_diff--;
	}
	while (hour_diff < 0) {
		hour_diff += HOURS_PER_DAY;
		day_diff--;
	}
	while (day_diff < 0) {
		if (timestamp_1 < timestamp_2) {
			day_diff += days_per_month[isleap(year1)][month1 - 1];
			month_diff--;
		} else {
			day_diff += days_per_month[isleap(year2)][month2 - 1];
			month_diff--;
		}
	}
	while (month_diff < 0) {
		month_diff += MONTHS_PER_YEAR;
		year_diff--;
	}

	// recover sign if necessary
	if (timestamp_1 < timestamp_2 && (month_diff != 0 || day_diff != 0)) {
		year_diff = -year_diff;
		month_diff = -month_diff;
		day_diff = -day_diff;
		hour_diff = -hour_diff;
		min_diff = -min_diff;
		sec_diff = -sec_diff;
		msec_diff = -msec_diff;
	}
	interval_t interval;
	interval.months = year_diff * MONTHS_PER_YEAR + month_diff;
	interval.days = day_diff;
	interval.msecs =
	    ((((((hour_diff * MINS_PER_HOUR) + min_diff) * SECS_PER_MINUTE) + sec_diff) * MSECS_PER_SEC) + msec_diff);

	return interval;
}


}
