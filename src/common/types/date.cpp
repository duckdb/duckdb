#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/limits.hpp"

#include <cstring>
#include <cctype>
#include <algorithm>

namespace duckdb {
using namespace std;

string_t Date::MonthNamesAbbreviated[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
string_t Date::MonthNames[] = {"January", "February", "March",     "April",   "May",      "June",
                               "July",    "August",   "September", "October", "November", "December"};
string_t Date::DayNames[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
string_t Date::DayNamesAbbreviated[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

// Taken from MonetDB mtime.c

static int NORMALDAYS[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static int LEAPDAYS[13] = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static int CUMDAYS[13] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
static int CUMLEAPDAYS[13] = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};

#define YEAR_MAX 5867411
#define YEAR_MIN (-YEAR_MAX)
#define MONTHDAYS(m, y) ((m) != 2 ? LEAPDAYS[m] : leapyear(y) ? 29 : 28)
#define YEARDAYS(y) (leapyear(y) ? 366 : 365)
#define DD_DATE(d, m, y)                                                                                               \
	((m) > 0 && (m) <= 12 && (d) > 0 && (y) != 0 && (y) >= YEAR_MIN && (y) <= YEAR_MAX && (d) <= MONTHDAYS(m, y))
#define LOWER(c) ((c) >= 'A' && (c) <= 'Z' ? (c) + 'a' - 'A' : (c))
// 1970-01-01 in date_t format
#define EPOCH_DATE 719528
// 1970-01-01 was a Thursday
#define EPOCH_DAY_OF_THE_WEEK 4
#define SECONDS_PER_DAY (60 * 60 * 24)

#define leapyear(y) ((y) % 4 == 0 && ((y) % 100 != 0 || (y) % 400 == 0))

static inline int leapyears(int year) {
	/* count the 4-fold years that passed since jan-1-0 */
	int y4 = year / 4;

	/* count the 100-fold years */
	int y100 = year / 100;

	/* count the 400-fold years */
	int y400 = year / 400;

	return y4 + y400 - y100 + (year >= 0); /* may be negative */
}

static inline void number_to_date(int32_t n, int32_t &year, int32_t &month, int32_t &day) {
	year = n / 365;
	day = (n - year * 365) - leapyears(year >= 0 ? year - 1 : year);
	if (n < 0) {
		year--;
		while (day >= 0) {
			year++;
			day -= YEARDAYS(year);
		}
		day = YEARDAYS(year) + day;
	} else {
		while (day < 0) {
			year--;
			day += YEARDAYS(year);
		}
	}

	day++;
	if (leapyear(year)) {
		for (month = day / 31 == 0 ? 1 : day / 31; month <= 12; month++)
			if (day > CUMLEAPDAYS[month - 1] && day <= CUMLEAPDAYS[month]) {
				break;
			}
		day -= CUMLEAPDAYS[month - 1];
	} else {
		for (month = day / 31 == 0 ? 1 : day / 31; month <= 12; month++)
			if (day > CUMDAYS[month - 1] && day <= CUMDAYS[month]) {
				break;
			}
		day -= CUMDAYS[month - 1];
	}
	year = (year <= 0) ? year - 1 : year;
}

static inline int32_t date_to_number(int32_t year, int32_t month, int32_t day) {
	int32_t n = 0;
	if (!(DD_DATE(day, month, year))) {
		throw ConversionException("Date out of range: %d-%d-%d", year, month, day);
	}

	if (year < 0)
		year++;
	n = (int32_t)(day - 1);
	if (month > 2 && leapyear(year)) {
		n++;
	}
	n += CUMDAYS[month - 1];
	/* current year does not count as leapyear */
	n += 365 * year + leapyears(year >= 0 ? year - 1 : year);

	return n;
}

static bool ParseDoubleDigit(const char *buf, idx_t len, idx_t &pos, int32_t &result) {
	if (pos < len && StringUtil::CharacterIsDigit(buf[pos])) {
		result = buf[pos++] - '0';
		if (pos < len && StringUtil::CharacterIsDigit(buf[pos])) {
			result = (buf[pos++] - '0') + result * 10;
		}
		return true;
	}
	return false;
}

bool Date::TryConvertDate(const char *buf, idx_t len, idx_t &pos, date_t &result, bool strict) {

	pos = 0;
	if (len == 0) {
		return false;
	}

	int32_t day = 0, month = -1;
	int32_t year = 0, yearneg = (buf[0] == '-');
	int sep;

	// skip leading spaces
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}

	if (pos >= len) {
		return false;
	}

	if (yearneg == 0 && !StringUtil::CharacterIsDigit(buf[pos])) {
		return false;
	}

	// first parse the year
	for (pos = pos + yearneg; pos < len && StringUtil::CharacterIsDigit(buf[pos]); pos++) {
		year = (buf[pos] - '0') + year * 10;
		if (year > YEAR_MAX) {
			break;
		}
	}

	if (pos >= len) {
		return false;
	}

	// fetch the separator
	sep = buf[pos++];
	if (sep != ' ' && sep != '-' && sep != '/' && sep != '\\') {
		// invalid separator
		return false;
	}

	// parse the month
	if (!ParseDoubleDigit(buf, len, pos, month)) {
		return false;
	}

	if (pos >= len) {
		return false;
	}

	if (buf[pos++] != sep) {
		return false;
	}

	if (pos >= len) {
		return false;
	}

	// now parse the day
	if (!ParseDoubleDigit(buf, len, pos, day)) {
		return false;
	}

	// check for an optional trailing " (BC)""
	if (len - pos > 5 && StringUtil::CharacterIsSpace(buf[pos]) && buf[pos + 1] == '(' && buf[pos + 2] == 'B' &&
	    buf[pos + 3] == 'C' && buf[pos + 4] == ')') {
		year = -year;
		pos += 5;
	}

	// in strict mode, check remaining string for non-space characters
	if (strict) {
		// skip trailing spaces
		while (pos < len && StringUtil::CharacterIsSpace((unsigned char)buf[pos])) {
			pos++;
		}
		// check position. if end was not reached, non-space chars remaining
		if (pos < len) {
			return false;
		}
	} else {
		// in non-strict mode, check for any direct trailing digits
		if (pos < len && StringUtil::CharacterIsDigit((unsigned char)buf[pos])) {
			return false;
		}
	}

	result = Date::FromDate(yearneg ? -year : year, month, day);
	return true;
}

date_t Date::FromCString(const char *buf, idx_t len, bool strict) {
	date_t result;
	idx_t pos;
	if (!TryConvertDate(buf, len, pos, result, strict)) {
		throw ConversionException("date/time field value out of range: \"%s\", "
		                          "expected format is (YYYY-MM-DD)",
		                          string(buf, len));
	}
	return result;
}

date_t Date::FromString(string str, bool strict) {
	return Date::FromCString(str.c_str(), str.size(), strict);
}

string Date::ToString(int32_t date) {
	int32_t year, month, day;
	number_to_date(date, year, month, day);
	if (year < 0) {
		return StringUtil::Format("%04d-%02d-%02d (BC)", -year, month, day);
	} else {
		return StringUtil::Format("%04d-%02d-%02d", year, month, day);
	}
}

string Date::Format(int32_t year, int32_t month, int32_t day) {
	return ToString(Date::FromDate(year, month, day));
}

void Date::Convert(int32_t date, int32_t &out_year, int32_t &out_month, int32_t &out_day) {
	number_to_date(date, out_year, out_month, out_day);
}

int32_t Date::FromDate(int32_t year, int32_t month, int32_t day) {
	return date_to_number(year, month, day);
}

bool Date::IsLeapYear(int32_t year) {
	return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

bool Date::IsValidDay(int32_t year, int32_t month, int32_t day) {
	if (month < 1 || month > 12)
		return false;
	if (day < 1)
		return false;
	if (year < YEAR_MIN || year > YEAR_MAX)
		return false;

	return IsLeapYear(year) ? day <= LEAPDAYS[month] : day <= NORMALDAYS[month];
}

date_t Date::EpochDaysToDate(int32_t epoch) {
	D_ASSERT(epoch + EPOCH_DATE <= NumericLimits<int32_t>::Maximum());
	return (date_t)(epoch + EPOCH_DATE);
}

int32_t Date::EpochDays(date_t date) {
	return ((int32_t)date - EPOCH_DATE);
}

date_t Date::EpochToDate(int64_t epoch) {
	D_ASSERT((epoch / SECONDS_PER_DAY) + EPOCH_DATE <= NumericLimits<int32_t>::Maximum());
	return (date_t)((epoch / SECONDS_PER_DAY) + EPOCH_DATE);
}

int64_t Date::Epoch(date_t date) {
	return ((int64_t)date - EPOCH_DATE) * SECONDS_PER_DAY;
}

static date_t common_year_days[] { 726102, 726468, 726833, 727198, 727563, 727929, 728294, 728659, 729024, 729390, 729755, 730120, 730485, 730851, 731216, 731581, 731946, 732312, 732677, 733042, 733407, 733773, 734138, 734503, 734868, 735234, 735599, 735964, 736329, 736695, 737060, 737425, 737790, 738156, 738521, 738886, 739251, 739617, 739982, 740347, 740712, 741078, 741443, 741808, 742173, 742539, 742904, 743269, 743634, 744000, 744365, 744730, 745095, 745461, 745826, 746191, 746556, 746922, 747287, 747652, 748017, 748383, 748748, 749113};


int32_t Date::ExtractYear(date_t n, int32_t *last_year) {
	if (n >= common_year_days[*last_year] && n < common_year_days[*last_year + 1]) {
		return 1988 + *last_year;
	}

	if (n < common_year_days[32]) {
		if (n < common_year_days[16]) {
			if (n < common_year_days[8]) {
				if (n >= common_year_days[0]) {
					date_t result = 1988;
					result += n >= common_year_days[1];
					result += n >= common_year_days[2];
					result += n >= common_year_days[3];
					result += n >= common_year_days[4];
					result += n >= common_year_days[5];
					result += n >= common_year_days[6];
					result += n >= common_year_days[7];
					*last_year = result - 1988;
					return result;
				}
			} else {
				date_t result = 1996;
				result += n >= common_year_days[9];
				result += n >= common_year_days[10];
				result += n >= common_year_days[11];
				result += n >= common_year_days[12];
				result += n >= common_year_days[13];
				result += n >= common_year_days[14];
				result += n >= common_year_days[15];
				*last_year = result - 1988;
				return result;
			}
		} else {
			if (n < common_year_days[24]) {
				date_t result = 2004;
				result += n >= common_year_days[17];
				result += n >= common_year_days[18];
				result += n >= common_year_days[19];
				result += n >= common_year_days[20];
				result += n >= common_year_days[21];
				result += n >= common_year_days[22];
				result += n >= common_year_days[23];
				*last_year = result - 1988;
				return result;
			} else {
				date_t result = 2012;
				result += n >= common_year_days[25];
				result += n >= common_year_days[26];
				result += n >= common_year_days[27];
				result += n >= common_year_days[28];
				result += n >= common_year_days[29];
				result += n >= common_year_days[30];
				result += n >= common_year_days[31];
				*last_year = result - 1988;
				return result;
			}
		}
	} else {
		if (n < common_year_days[48]) {
			if (n < common_year_days[40]) {
				date_t result = 2020;
				result += n >= common_year_days[33];
				result += n >= common_year_days[34];
				result += n >= common_year_days[35];
				result += n >= common_year_days[36];
				result += n >= common_year_days[37];
				result += n >= common_year_days[38];
				result += n >= common_year_days[39];
				*last_year = result - 1988;
				return result;
			} else {
				date_t result = 2028;
				result += n >= common_year_days[41];
				result += n >= common_year_days[42];
				result += n >= common_year_days[43];
				result += n >= common_year_days[44];
				result += n >= common_year_days[45];
				result += n >= common_year_days[46];
				result += n >= common_year_days[47];
				*last_year = result - 1988;
				return result;
			}
		} else {
			if (n < common_year_days[56]) {
				date_t result = 2036;
				result += n >= common_year_days[49];
				result += n >= common_year_days[50];
				result += n >= common_year_days[51];
				result += n >= common_year_days[52];
				result += n >= common_year_days[53];
				result += n >= common_year_days[54];
				result += n >= common_year_days[55];
				*last_year = result - 1988;
				return result;
			} else if (n < common_year_days[63]) {
				date_t result = 2044;
				result += n >= common_year_days[57];
				result += n >= common_year_days[58];
				result += n >= common_year_days[59];
				result += n >= common_year_days[60];
				result += n >= common_year_days[61];
				result += n >= common_year_days[62];
				*last_year = result - 1988;
				return result;
			}
		}
	}
	// slower fallback for dates out of the supported common year range
	return Date::ExtractYear(n);
}

int32_t Date::ExtractYear(timestamp_t ts, int32_t *last_year) {
	return Date::ExtractYear(Timestamp::GetDate(ts), last_year);
}

int32_t Date::ExtractYear(date_t n) {
	int32_t year = n / 365;
	int32_t day = (n - year * 365) - leapyears(year >= 0 ? year - 1 : year);
	if (n < 0) {
		year--;
		while (day >= 0) {
			year++;
			day -= YEARDAYS(year);
		}
		day = YEARDAYS(year) + day;
	} else {
		while (day < 0) {
			year--;
			day += YEARDAYS(year);
		}
	}
	return (year <= 0) ? year - 1 : year;
}

int32_t Date::ExtractMonth(date_t date) {
	int32_t out_year, out_month, out_day;
	Date::Convert(date, out_year, out_month, out_day);
	return out_month;
}

int32_t Date::ExtractDay(date_t date) {
	int32_t out_year, out_month, out_day;
	Date::Convert(date, out_year, out_month, out_day);
	return out_day;
}

int32_t Date::ExtractDayOfTheYear(date_t date) {
	int32_t out_year, out_month, out_day;
	Date::Convert(date, out_year, out_month, out_day);
	if (out_month >= 1) {
		out_day += Date::IsLeapYear(out_year) ? CUMLEAPDAYS[out_month - 1] : CUMDAYS[out_month - 1];
	}
	return out_day;
}

int32_t Date::ExtractISODayOfTheWeek(date_t date) {
	// -1 = 5
	// 0 = 6
	// 1 = 7
	// 2 = 1
	// 3 = 2
	// 4 = 3
	// 5 = 4
	// 6 = 5
	// 0 = 6
	// 1 = 7
	if (date < 2) {
		return ((date - 1) % 7) + 7;
	} else {
		return ((date - 2) % 7) + 1;
	}
}

static int32_t GetISOWeek(int32_t year, int32_t month, int32_t day) {
	auto day_of_the_year = (Date::IsLeapYear(year) ? CUMLEAPDAYS[month] : CUMDAYS[month]) + day;
	// get the first day of the first week of the year
	// the first week is the week that has the 4th of January in it
	auto day_of_the_fourth = Date::ExtractISODayOfTheWeek(Date::FromDate(year, 1, 4));
	// if fourth is monday, then fourth is the first day
	// if fourth is tuesday, third is the first day
	// if fourth is wednesday, second is the first day
	// if fourth is thursday - sunday, first is the first day
	auto first_day_of_the_first_week = day_of_the_fourth >= 4 ? 0 : 5 - day_of_the_fourth;
	if (day_of_the_year < first_day_of_the_first_week) {
		// day is part of last year
		return GetISOWeek(year - 1, 12, day);
	} else {
		return ((day_of_the_year - first_day_of_the_first_week) / 7) + 1;
	}
}

int32_t Date::ExtractISOWeekNumber(date_t date) {
	int32_t year, month, day;
	Date::Convert(date, year, month, day);
	return GetISOWeek(year, month - 1, day - 1);
}

int32_t Date::ExtractWeekNumberRegular(date_t date, bool monday_first) {
	int32_t year, month, day;
	Date::Convert(date, year, month, day);
	month -= 1;
	day -= 1;
	// get the day of the year
	auto day_of_the_year = (Date::IsLeapYear(year) ? CUMLEAPDAYS[month] : CUMDAYS[month]) + day;
	// now figure out the first monday or sunday of the year
	// what day is January 1st?
	auto day_of_jan_first = Date::ExtractISODayOfTheWeek(Date::FromDate(year, 1, 1));
	// monday = 1, sunday = 7
	int32_t first_week_start;
	if (monday_first) {
		// have to find next "1"
		if (day_of_jan_first == 1) {
			// jan 1 is monday: starts immediately
			first_week_start = 0;
		} else {
			// jan 1 is not monday: count days until next monday
			first_week_start = 8 - day_of_jan_first;
		}
	} else {
		first_week_start = 7 - day_of_jan_first;
	}
	if (day_of_the_year < first_week_start) {
		// day occurs before first week starts: week 0
		return 0;
	}
	return ((day_of_the_year - first_week_start) / 7) + 1;
}

// Returns the date of the monday of the current week.
date_t Date::GetMondayOfCurrentWeek(date_t date) {
	int32_t dotw = Date::ExtractISODayOfTheWeek(date);

	int32_t days = date_to_number(Date::ExtractYear(date), Date::ExtractMonth(date), Date::ExtractDay(date));

	days -= dotw - 1;

	int32_t year, month, day;
	number_to_date(days, year, month, day);

	return (Date::FromDate(year, month, day));
}

} // namespace duckdb
