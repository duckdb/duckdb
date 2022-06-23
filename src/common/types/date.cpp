#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/limits.hpp"

#include <cstring>
#include <cctype>
#include <algorithm>

namespace duckdb {

static_assert(sizeof(date_t) == sizeof(int32_t), "date_t was padded");

const char *Date::PINF = "infinity";  // NOLINT
const char *Date::NINF = "-infinity"; // NOLINT
const char *Date::EPOCH = "epoch";    // NOLINT

const string_t Date::MONTH_NAMES_ABBREVIATED[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
const string_t Date::MONTH_NAMES[] = {"January", "February", "March",     "April",   "May",      "June",
                                      "July",    "August",   "September", "October", "November", "December"};
const string_t Date::DAY_NAMES[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
const string_t Date::DAY_NAMES_ABBREVIATED[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

const int32_t Date::NORMAL_DAYS[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
const int32_t Date::CUMULATIVE_DAYS[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
const int32_t Date::LEAP_DAYS[] = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
const int32_t Date::CUMULATIVE_LEAP_DAYS[] = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};
const int8_t Date::MONTH_PER_DAY_OF_YEAR[] = {
    1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    1,  1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    3,  3,  3,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    4,  4,  4,  4,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
    5,  5,  5,  5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
    6,  6,  6,  6,  6,  6,  6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
    7,  7,  7,  7,  7,  7,  7,  7,  7,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,
    8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
    9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
    11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12};
const int8_t Date::LEAP_MONTH_PER_DAY_OF_YEAR[] = {
    1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    1,  1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  2,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    3,  3,  3,  3,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    4,  4,  4,  4,  4,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
    5,  5,  5,  5,  5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
    6,  6,  6,  6,  6,  6,  6,  6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
    7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,
    8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
    9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
    11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12};
const int32_t Date::CUMULATIVE_YEAR_DAYS[] = {
    0,      365,    730,    1096,   1461,   1826,   2191,   2557,   2922,   3287,   3652,   4018,   4383,   4748,
    5113,   5479,   5844,   6209,   6574,   6940,   7305,   7670,   8035,   8401,   8766,   9131,   9496,   9862,
    10227,  10592,  10957,  11323,  11688,  12053,  12418,  12784,  13149,  13514,  13879,  14245,  14610,  14975,
    15340,  15706,  16071,  16436,  16801,  17167,  17532,  17897,  18262,  18628,  18993,  19358,  19723,  20089,
    20454,  20819,  21184,  21550,  21915,  22280,  22645,  23011,  23376,  23741,  24106,  24472,  24837,  25202,
    25567,  25933,  26298,  26663,  27028,  27394,  27759,  28124,  28489,  28855,  29220,  29585,  29950,  30316,
    30681,  31046,  31411,  31777,  32142,  32507,  32872,  33238,  33603,  33968,  34333,  34699,  35064,  35429,
    35794,  36160,  36525,  36890,  37255,  37621,  37986,  38351,  38716,  39082,  39447,  39812,  40177,  40543,
    40908,  41273,  41638,  42004,  42369,  42734,  43099,  43465,  43830,  44195,  44560,  44926,  45291,  45656,
    46021,  46387,  46752,  47117,  47482,  47847,  48212,  48577,  48942,  49308,  49673,  50038,  50403,  50769,
    51134,  51499,  51864,  52230,  52595,  52960,  53325,  53691,  54056,  54421,  54786,  55152,  55517,  55882,
    56247,  56613,  56978,  57343,  57708,  58074,  58439,  58804,  59169,  59535,  59900,  60265,  60630,  60996,
    61361,  61726,  62091,  62457,  62822,  63187,  63552,  63918,  64283,  64648,  65013,  65379,  65744,  66109,
    66474,  66840,  67205,  67570,  67935,  68301,  68666,  69031,  69396,  69762,  70127,  70492,  70857,  71223,
    71588,  71953,  72318,  72684,  73049,  73414,  73779,  74145,  74510,  74875,  75240,  75606,  75971,  76336,
    76701,  77067,  77432,  77797,  78162,  78528,  78893,  79258,  79623,  79989,  80354,  80719,  81084,  81450,
    81815,  82180,  82545,  82911,  83276,  83641,  84006,  84371,  84736,  85101,  85466,  85832,  86197,  86562,
    86927,  87293,  87658,  88023,  88388,  88754,  89119,  89484,  89849,  90215,  90580,  90945,  91310,  91676,
    92041,  92406,  92771,  93137,  93502,  93867,  94232,  94598,  94963,  95328,  95693,  96059,  96424,  96789,
    97154,  97520,  97885,  98250,  98615,  98981,  99346,  99711,  100076, 100442, 100807, 101172, 101537, 101903,
    102268, 102633, 102998, 103364, 103729, 104094, 104459, 104825, 105190, 105555, 105920, 106286, 106651, 107016,
    107381, 107747, 108112, 108477, 108842, 109208, 109573, 109938, 110303, 110669, 111034, 111399, 111764, 112130,
    112495, 112860, 113225, 113591, 113956, 114321, 114686, 115052, 115417, 115782, 116147, 116513, 116878, 117243,
    117608, 117974, 118339, 118704, 119069, 119435, 119800, 120165, 120530, 120895, 121260, 121625, 121990, 122356,
    122721, 123086, 123451, 123817, 124182, 124547, 124912, 125278, 125643, 126008, 126373, 126739, 127104, 127469,
    127834, 128200, 128565, 128930, 129295, 129661, 130026, 130391, 130756, 131122, 131487, 131852, 132217, 132583,
    132948, 133313, 133678, 134044, 134409, 134774, 135139, 135505, 135870, 136235, 136600, 136966, 137331, 137696,
    138061, 138427, 138792, 139157, 139522, 139888, 140253, 140618, 140983, 141349, 141714, 142079, 142444, 142810,
    143175, 143540, 143905, 144271, 144636, 145001, 145366, 145732, 146097};

void Date::ExtractYearOffset(int32_t &n, int32_t &year, int32_t &year_offset) {
	year = Date::EPOCH_YEAR;
	// first we normalize n to be in the year range [1970, 2370]
	// since leap years repeat every 400 years, we can safely normalize just by "shifting" the CumulativeYearDays array
	while (n < 0) {
		n += Date::DAYS_PER_YEAR_INTERVAL;
		year -= Date::YEAR_INTERVAL;
	}
	while (n >= Date::DAYS_PER_YEAR_INTERVAL) {
		n -= Date::DAYS_PER_YEAR_INTERVAL;
		year += Date::YEAR_INTERVAL;
	}
	// interpolation search
	// we can find an upper bound of the year by assuming each year has 365 days
	year_offset = n / 365;
	// because of leap years we might be off by a little bit: compensate by decrementing the year offset until we find
	// our year
	while (n < Date::CUMULATIVE_YEAR_DAYS[year_offset]) {
		year_offset--;
		D_ASSERT(year_offset >= 0);
	}
	year += year_offset;
	D_ASSERT(n >= Date::CUMULATIVE_YEAR_DAYS[year_offset]);
}

void Date::Convert(date_t d, int32_t &year, int32_t &month, int32_t &day) {
	auto n = d.days;
	int32_t year_offset;
	Date::ExtractYearOffset(n, year, year_offset);

	day = n - Date::CUMULATIVE_YEAR_DAYS[year_offset];
	D_ASSERT(day >= 0 && day <= 365);

	bool is_leap_year = (Date::CUMULATIVE_YEAR_DAYS[year_offset + 1] - Date::CUMULATIVE_YEAR_DAYS[year_offset]) == 366;
	if (is_leap_year) {
		month = Date::LEAP_MONTH_PER_DAY_OF_YEAR[day];
		day -= Date::CUMULATIVE_LEAP_DAYS[month - 1];
	} else {
		month = Date::MONTH_PER_DAY_OF_YEAR[day];
		day -= Date::CUMULATIVE_DAYS[month - 1];
	}
	day++;
	D_ASSERT(day > 0 && day <= (is_leap_year ? Date::LEAP_DAYS[month] : Date::NORMAL_DAYS[month]));
	D_ASSERT(month > 0 && month <= 12);
}

bool Date::TryFromDate(int32_t year, int32_t month, int32_t day, date_t &result) {
	int32_t n = 0;
	if (!Date::IsValid(year, month, day)) {
		return false;
	}
	n += Date::IsLeapYear(year) ? Date::CUMULATIVE_LEAP_DAYS[month - 1] : Date::CUMULATIVE_DAYS[month - 1];
	n += day - 1;
	if (year < 1970) {
		int32_t diff_from_base = 1970 - year;
		int32_t year_index = 400 - (diff_from_base % 400);
		int32_t fractions = diff_from_base / 400;
		n += Date::CUMULATIVE_YEAR_DAYS[year_index];
		n -= Date::DAYS_PER_YEAR_INTERVAL;
		n -= fractions * Date::DAYS_PER_YEAR_INTERVAL;
	} else if (year >= 2370) {
		int32_t diff_from_base = year - 2370;
		int32_t year_index = diff_from_base % 400;
		int32_t fractions = diff_from_base / 400;
		n += Date::CUMULATIVE_YEAR_DAYS[year_index];
		n += Date::DAYS_PER_YEAR_INTERVAL;
		n += fractions * Date::DAYS_PER_YEAR_INTERVAL;
	} else {
		n += Date::CUMULATIVE_YEAR_DAYS[year - 1970];
	}
#ifdef DEBUG
	int32_t y, m, d;
	Date::Convert(date_t(n), y, m, d);
	D_ASSERT(year == y);
	D_ASSERT(month == m);
	D_ASSERT(day == d);
#endif
	result = date_t(n);
	return true;
}

date_t Date::FromDate(int32_t year, int32_t month, int32_t day) {
	date_t result;
	if (!Date::TryFromDate(year, month, day, result)) {
		throw ConversionException("Date out of range: %d-%d-%d", year, month, day);
	}
	return result;
}

bool Date::ParseDoubleDigit(const char *buf, idx_t len, idx_t &pos, int32_t &result) {
	if (pos < len && StringUtil::CharacterIsDigit(buf[pos])) {
		result = buf[pos++] - '0';
		if (pos < len && StringUtil::CharacterIsDigit(buf[pos])) {
			result = (buf[pos++] - '0') + result * 10;
		}
		return true;
	}
	return false;
}

static bool TryConvertDateSpecial(const char *buf, idx_t len, idx_t &pos, const char *special) {
	auto p = pos;
	for (; p < len && *special; ++p) {
		const auto s = *special++;
		if (!s || StringUtil::CharacterToLower(buf[p]) != s) {
			return false;
		}
	}
	pos = p;
	return true;
}

bool Date::TryConvertDate(const char *buf, idx_t len, idx_t &pos, date_t &result, bool strict) {
	pos = 0;
	if (len == 0) {
		return false;
	}

	int32_t day = 0;
	int32_t month = -1;
	int32_t year = 0;
	bool yearneg = false;
	int sep;

	// skip leading spaces
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}

	if (pos >= len) {
		return false;
	}
	if (buf[pos] == '-') {
		yearneg = true;
		pos++;
		if (pos >= len) {
			return false;
		}
	}
	if (!StringUtil::CharacterIsDigit(buf[pos])) {
		// Check for special values
		if (TryConvertDateSpecial(buf, len, pos, PINF)) {
			result = yearneg ? date_t::ninfinity() : date_t::infinity();
		} else if (TryConvertDateSpecial(buf, len, pos, EPOCH)) {
			result = date_t::epoch();
		} else {
			return false;
		}
		// skip trailing spaces - parsing must be strict here
		while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
			pos++;
		}
		return pos == len;
	}
	// first parse the year
	for (; pos < len && StringUtil::CharacterIsDigit(buf[pos]); pos++) {
		if (year >= 100000000) {
			return false;
		}
		year = (buf[pos] - '0') + year * 10;
	}
	if (yearneg) {
		year = -year;
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
	if (!Date::ParseDoubleDigit(buf, len, pos, month)) {
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
	if (!Date::ParseDoubleDigit(buf, len, pos, day)) {
		return false;
	}

	// check for an optional trailing " (BC)""
	if (len - pos >= 5 && StringUtil::CharacterIsSpace(buf[pos]) && buf[pos + 1] == '(' &&
	    StringUtil::CharacterToLower(buf[pos + 2]) == 'b' && StringUtil::CharacterToLower(buf[pos + 3]) == 'c' &&
	    buf[pos + 4] == ')') {
		if (yearneg || year == 0) {
			return false;
		}
		year = -year + 1;
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

	return Date::TryFromDate(year, month, day, result);
}

string Date::ConversionError(const string &str) {
	return StringUtil::Format("date field value out of range: \"%s\", "
	                          "expected format is (YYYY-MM-DD)",
	                          str);
}

string Date::ConversionError(string_t str) {
	return ConversionError(str.GetString());
}

date_t Date::FromCString(const char *buf, idx_t len, bool strict) {
	date_t result;
	idx_t pos;
	if (!TryConvertDate(buf, len, pos, result, strict)) {
		throw ConversionException(ConversionError(string(buf, len)));
	}
	return result;
}

date_t Date::FromString(const string &str, bool strict) {
	return Date::FromCString(str.c_str(), str.size(), strict);
}

string Date::ToString(date_t date) {
	// PG displays temporal infinities in lowercase,
	// but numerics in Titlecase.
	if (date == date_t::infinity()) {
		return PINF;
	} else if (date == date_t::ninfinity()) {
		return NINF;
	}
	int32_t date_units[3];
	idx_t year_length;
	bool add_bc;
	Date::Convert(date, date_units[0], date_units[1], date_units[2]);

	auto length = DateToStringCast::Length(date_units, year_length, add_bc);
	auto buffer = unique_ptr<char[]>(new char[length]);
	DateToStringCast::Format(buffer.get(), date_units, year_length, add_bc);
	return string(buffer.get(), length);
}

string Date::Format(int32_t year, int32_t month, int32_t day) {
	return ToString(Date::FromDate(year, month, day));
}

bool Date::IsLeapYear(int32_t year) {
	return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

bool Date::IsValid(int32_t year, int32_t month, int32_t day) {
	if (month < 1 || month > 12) {
		return false;
	}
	if (day < 1) {
		return false;
	}
	if (year <= DATE_MIN_YEAR) {
		if (year < DATE_MIN_YEAR) {
			return false;
		} else if (year == DATE_MIN_YEAR) {
			if (month < DATE_MIN_MONTH || (month == DATE_MIN_MONTH && day < DATE_MIN_DAY)) {
				return false;
			}
		}
	}
	if (year >= DATE_MAX_YEAR) {
		if (year > DATE_MAX_YEAR) {
			return false;
		} else if (year == DATE_MAX_YEAR) {
			if (month > DATE_MAX_MONTH || (month == DATE_MAX_MONTH && day > DATE_MAX_DAY)) {
				return false;
			}
		}
	}
	return Date::IsLeapYear(year) ? day <= Date::LEAP_DAYS[month] : day <= Date::NORMAL_DAYS[month];
}

int32_t Date::MonthDays(int32_t year, int32_t month) {
	D_ASSERT(month >= 1 && month <= 12);
	return Date::IsLeapYear(year) ? Date::LEAP_DAYS[month] : Date::NORMAL_DAYS[month];
}

date_t Date::EpochDaysToDate(int32_t epoch) {
	return (date_t)epoch;
}

int32_t Date::EpochDays(date_t date) {
	return date.days;
}

date_t Date::EpochToDate(int64_t epoch) {
	return date_t(epoch / Interval::SECS_PER_DAY);
}

int64_t Date::Epoch(date_t date) {
	return ((int64_t)date.days) * Interval::SECS_PER_DAY;
}

int64_t Date::EpochNanoseconds(date_t date) {
	return ((int64_t)date.days) * (Interval::MICROS_PER_DAY * 1000);
}

int32_t Date::ExtractYear(date_t d, int32_t *last_year) {
	auto n = d.days;
	// cached look up: check if year of this date is the same as the last one we looked up
	// note that this only works for years in the range [1970, 2370]
	if (n >= Date::CUMULATIVE_YEAR_DAYS[*last_year] && n < Date::CUMULATIVE_YEAR_DAYS[*last_year + 1]) {
		return Date::EPOCH_YEAR + *last_year;
	}
	int32_t year;
	Date::ExtractYearOffset(n, year, *last_year);
	return year;
}

int32_t Date::ExtractYear(timestamp_t ts, int32_t *last_year) {
	return Date::ExtractYear(Timestamp::GetDate(ts), last_year);
}

int32_t Date::ExtractYear(date_t d) {
	int32_t year, year_offset;
	Date::ExtractYearOffset(d.days, year, year_offset);
	return year;
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
	int32_t year, year_offset;
	Date::ExtractYearOffset(date.days, year, year_offset);
	return date.days - Date::CUMULATIVE_YEAR_DAYS[year_offset] + 1;
}

int32_t Date::ExtractISODayOfTheWeek(date_t date) {
	// date of 0 is 1970-01-01, which was a Thursday (4)
	// -7 = 4
	// -6 = 5
	// -5 = 6
	// -4 = 7
	// -3 = 1
	// -2 = 2
	// -1 = 3
	// 0  = 4
	// 1  = 5
	// 2  = 6
	// 3  = 7
	// 4  = 1
	// 5  = 2
	// 6  = 3
	// 7  = 4
	if (date.days < 0) {
		// negative date: start off at 4 and cycle downwards
		return (7 - ((-int64_t(date.days) + 3) % 7));
	} else {
		// positive date: start off at 4 and cycle upwards
		return ((int64_t(date.days) + 3) % 7) + 1;
	}
}

static int32_t GetISOYearWeek(int32_t &year, int32_t month, int32_t day) {
	auto day_of_the_year =
	    (Date::IsLeapYear(year) ? Date::CUMULATIVE_LEAP_DAYS[month] : Date::CUMULATIVE_DAYS[month]) + day;
	// get the first day of the first week of the year
	// the first week is the week that has the 4th of January in it
	const auto weekday_of_the_fourth = Date::ExtractISODayOfTheWeek(Date::FromDate(year, 1, 4));
	// if fourth is monday, then fourth is the first day
	// if fourth is tuesday, third is the first day
	// if fourth is wednesday, second is the first day
	// if fourth is thursday, first is the first day
	// if fourth is friday - sunday, day is in the previous year
	// (day is 0-based, weekday is 1-based)
	const auto first_day_of_the_first_isoweek = 4 - weekday_of_the_fourth;
	if (day_of_the_year < first_day_of_the_first_isoweek) {
		// day is part of last year (13th month)
		--year;
		return GetISOYearWeek(year, 12, day);
	} else {
		return ((day_of_the_year - first_day_of_the_first_isoweek) / 7) + 1;
	}
}

void Date::ExtractISOYearWeek(date_t date, int32_t &year, int32_t &week) {
	int32_t month, day;
	Date::Convert(date, year, month, day);
	week = GetISOYearWeek(year, month - 1, day - 1);
}

int32_t Date::ExtractISOWeekNumber(date_t date) {
	int32_t year, week;
	ExtractISOYearWeek(date, year, week);
	return week;
}

int32_t Date::ExtractISOYearNumber(date_t date) {
	int32_t year, week;
	ExtractISOYearWeek(date, year, week);
	return year;
}

int32_t Date::ExtractWeekNumberRegular(date_t date, bool monday_first) {
	int32_t year, month, day;
	Date::Convert(date, year, month, day);
	month -= 1;
	day -= 1;
	// get the day of the year
	auto day_of_the_year =
	    (Date::IsLeapYear(year) ? Date::CUMULATIVE_LEAP_DAYS[month] : Date::CUMULATIVE_DAYS[month]) + day;
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
	return date - (dotw - 1);
}

} // namespace duckdb
