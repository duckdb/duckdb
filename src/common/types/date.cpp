
#include <time.h>

#include "common/types/date.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

// Taken from MonetDB mtime.c

static int NORMALDAYS[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static int LEAPDAYS[13] = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static int CUMDAYS[13] = {0,   31,  59,  90,  120, 151, 181,
                          212, 243, 273, 304, 334, 365};
static int CUMLEAPDAYS[13] = {0,   31,  60,  91,  121, 152, 182,
                              213, 244, 274, 305, 335, 366};

#define YEAR_MAX 5867411
#define YEAR_MIN (-YEAR_MAX)
#define MONTHDAYS(m, y) ((m) != 2 ? LEAPDAYS[m] : leapyear(y) ? 29 : 28)
#define YEARDAYS(y) (leapyear(y) ? 366 : 365)
#define DATE(d, m, y)                                                          \
	((m) > 0 && (m) <= 12 && (d) > 0 && (y) != 0 && (y) >= YEAR_MIN &&         \
	 (y) <= YEAR_MAX && (d) <= MONTHDAYS(m, y))
#define TIME(h, m, s, x)                                                       \
	((h) >= 0 && (h) < 24 && (m) >= 0 && (m) < 60 && (s) >= 0 && (s) < 60 &&   \
	 (x) >= 0 && (x) < 1000)
#define LOWER(c) ((c) >= 'A' && (c) <= 'Z' ? (c) + 'a' - 'A' : (c))

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

static inline void number_to_date(int32_t n, int32_t &year, int32_t &month,
                                  int32_t &day) {
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
	assert(DATE(day, month, year));

	if (year < 0)
		year++;
	n = (int32_t)(day - 1);
	if (month > 2 && leapyear(year))
		n++;
	n += CUMDAYS[month - 1];
	/* current year does not count as leapyear */
	n += 365 * year + leapyears(year >= 0 ? year - 1 : year);

	return n;
}

int32_t Date::FromString(string str) {
	struct tm tm;
	strptime(str.c_str(), "%Y-%m-%d", &tm);
	int32_t year = 1900 + tm.tm_year;
	int32_t month = 1 + tm.tm_mon;
	int32_t day = tm.tm_mday;
	if (!IsValidDay(year, month, day)) {
		throw ConversionException("date/time field value out of range: \"%s\", expected format is (YYYY-MM-DD)", str.c_str());
	}
	return Date::FromDate(year, month, day);
}

string Date::ToString(int32_t date) {
	struct tm tm;
	char buffer[100];
	int32_t year, month, day;
	number_to_date(date, year, month, day);

	tm.tm_year = year - 1900;
	tm.tm_mon = month - 1;
	tm.tm_mday = day;
	strftime(buffer, 100, "%Y-%m-%d", &tm);
	return std::string(buffer);
}

string Date::Format(int32_t year, int32_t month, int32_t day) {
	struct tm tm;
	char buffer[100];

	tm.tm_year = year - 1900;
	tm.tm_mon = month - 1;
	tm.tm_mday = day;

	strftime(buffer, 100, "%Y-%m-%d", &tm);
	return std::string(buffer);
}

void Date::Convert(int32_t date, int32_t &out_year, int32_t &out_month,
                   int32_t &out_day) {
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
