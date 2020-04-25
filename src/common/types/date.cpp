#include "duckdb/common/types/date.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <cctype>
#include <iomanip>
#include <iostream>
#include <sstream>

using namespace duckdb;
using namespace std;

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

static bool ParseDoubleDigit(const char *buf, idx_t &pos, int32_t &result) {
	if (std::isdigit((unsigned char)buf[pos])) {
		result = buf[pos++] - '0';
		if (std::isdigit((unsigned char)buf[pos])) {
			result = (buf[pos++] - '0') + result * 10;
		}
		return true;
	}
	return false;
}

static bool TryConvertDate(const char *buf, date_t &result) {
	int32_t day = 0, month = -1;
	int32_t year = 0, yearneg = (buf[0] == '-');
	idx_t pos = 0;
	int sep;

	if (yearneg == 0 && !std::isdigit((unsigned char)buf[0])) {
		return false;
	}

	// first parse the year
	for (pos = yearneg; std::isdigit((unsigned char)buf[pos]); pos++) {
		year = (buf[pos] - '0') + year * 10;
		if (year > YEAR_MAX) {
			break;
		}
	}

	// fetch the separator
	sep = buf[pos++];
	if (sep != ' ' && sep != '-' && sep != '/' && sep != '\\') {
		// invalid separator
		return false;
	}

	// parse the month
	if (!ParseDoubleDigit(buf, pos, month)) {
		return false;
	}

	if (buf[pos++] != sep) {
		return false;
	}

	// now parse the day
	if (!ParseDoubleDigit(buf, pos, day)) {
		return false;
	}

	if (std::isdigit((unsigned char)buf[pos])) {
		return false;
	}

	result = Date::FromDate(yearneg ? -year : year, month, day);
	return true;
}

date_t Date::FromCString(const char *buf) {
	date_t result;
	if (!TryConvertDate(buf, result)) {
		throw ConversionException("date/time field value out of range: \"%s\", "
		                          "expected format is (YYYY-MM-DD)",
		                          buf);
	}
	return result;
}

date_t Date::FromString(string str) {
	return Date::FromCString(str.c_str());
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

date_t Date::EpochToDate(int64_t epoch) {
	assert((epoch / SECONDS_PER_DAY) + EPOCH_DATE <= numeric_limits<int32_t>::max());
	return (date_t)((epoch / SECONDS_PER_DAY) + EPOCH_DATE);
}

int64_t Date::Epoch(date_t date) {
	return ((int64_t)date - EPOCH_DATE) * SECONDS_PER_DAY;
}
int32_t Date::ExtractYear(date_t date) {
	int32_t out_year, out_month, out_day;
	Date::Convert(date, out_year, out_month, out_day);
	return out_year;
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

static int32_t GetWeek(int32_t year, int32_t month, int32_t day) {
	auto day_of_the_year = (leapyear(year) ? CUMLEAPDAYS[month] : CUMDAYS[month]) + day;
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
		return GetWeek(year - 1, 12, day);
	} else {
		return ((day_of_the_year - first_day_of_the_first_week) / 7) + 1;
	}
}

int32_t Date::ExtractWeekNumber(date_t date) {
	int32_t year, month, day;
	Date::Convert(date, year, month, day);
	return GetWeek(year, month - 1, day - 1);
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
