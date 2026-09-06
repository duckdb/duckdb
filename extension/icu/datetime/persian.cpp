#include "persian.hpp"

#include "grego.hpp"

#include <algorithm>

namespace duckdb {
namespace datetime {

//===--------------------------------------------------------------------===//
// Persian
//===--------------------------------------------------------------------===//
//! The first day of the first Persian year, as a Julian day
static constexpr int32_t PERSIAN_EPOCH = 1948320;

//! The days before every 0-based month
static const int16_t PERSIAN_DAYS_BEFORE[] = {0, 31, 62, 93, 124, 155, 186, 216, 246, 276, 306, 336};
static const int8_t PERSIAN_MONTH_LENGTH[] = {31, 31, 31, 31, 31, 31, 30, 30, 30, 30, 30, 29};
static const int8_t PERSIAN_LEAP_MONTH_LENGTH[] = {31, 31, 31, 31, 31, 31, 30, 30, 30, 30, 30, 30};

static const int32_t PERSIAN_LIMITS[CAL_FIELD_COUNT][4] = {
    //  minimum, greatest minimum, least maximum, maximum
    {0, 0, 0, 0},                           // ERA
    {-5000000, -5000000, 5000000, 5000000}, // YEAR
    {0, 0, 11, 11},                         // MONTH
    {1, 1, 52, 53},                         // WEEK_OF_YEAR
    {-1, -1, -1, -1},                       // WEEK_OF_MONTH
    {1, 1, 29, 31},                         // DATE
    {1, 1, 365, 366},                       // DAY_OF_YEAR
    {-1, -1, -1, -1},                       // DAY_OF_WEEK
    {1, 1, 5, 5},                           // DAY_OF_WEEK_IN_MONTH
    {-1, -1, -1, -1},                       // AM_PM
    {-1, -1, -1, -1},                       // HOUR
    {-1, -1, -1, -1},                       // HOUR_OF_DAY
    {-1, -1, -1, -1},                       // MINUTE
    {-1, -1, -1, -1},                       // SECOND
    {-1, -1, -1, -1},                       // MILLISECOND
    {-1, -1, -1, -1},                       // ZONE_OFFSET
    {-1, -1, -1, -1},                       // DST_OFFSET
    {-5000000, -5000000, 5000000, 5000000}, // YEAR_WOY
    {-1, -1, -1, -1},                       // DOW_LOCAL
    {-5000000, -5000000, 5000000, 5000000}, // EXTENDED_YEAR
    {-1, -1, -1, -1},                       // JULIAN_DAY
    {-1, -1, -1, -1},                       // MILLISECONDS_IN_DAY
    {-1, -1, -1, -1},                       // IS_LEAP_MONTH
    {0, 0, 11, 11},                         // ORDINAL_MONTH
};

//! The years in which the 33 year cycle predicts a leap year that the astronomical calendar
//! does not have, in ascending order
static const int16_t PERSIAN_NON_LEAP_YEARS[] = {
    1502, 1601, 1634, 1667, 1700, 1733, 1766, 1799, 1832, 1865, 1898, 1931, 1964, 1997, 2030, 2059,
    2063, 2096, 2129, 2158, 2162, 2191, 2195, 2224, 2228, 2257, 2261, 2290, 2294, 2323, 2327, 2356,
    2360, 2389, 2393, 2422, 2426, 2455, 2459, 2488, 2492, 2521, 2525, 2554, 2558, 2587, 2591, 2620,
    2624, 2653, 2657, 2686, 2690, 2719, 2723, 2748, 2752, 2756, 2781, 2785, 2789, 2818, 2822, 2847,
    2851, 2855, 2880, 2884, 2888, 2913, 2917, 2921, 2946, 2950, 2954, 2979, 2983, 2987};

//! The first year that needs a correction
static const int32_t PERSIAN_MIN_CORRECTION = PERSIAN_NON_LEAP_YEARS[0];

static bool IsCorrectedYear(int64_t year) {
	if (year < PERSIAN_MIN_CORRECTION ||
	    year > PERSIAN_NON_LEAP_YEARS[std::end(PERSIAN_NON_LEAP_YEARS) - std::begin(PERSIAN_NON_LEAP_YEARS) - 1]) {
		return false;
	}
	return std::binary_search(std::begin(PERSIAN_NON_LEAP_YEARS), std::end(PERSIAN_NON_LEAP_YEARS), int16_t(year));
}

static bool IsPersianLeapYear(int32_t year) {
	if (year >= PERSIAN_MIN_CORRECTION && IsCorrectedYear(year)) {
		return false;
	}
	if (year > PERSIAN_MIN_CORRECTION && IsCorrectedYear(year - 1)) {
		return true;
	}
	return (int64_t(year) * 25LL + 11LL) % 33L < 8;
}

//! The day the year starts on, relative to the epoch
static int64_t FirstDayOfYear(int64_t year) {
	auto julian_day = 365 * (year - 1) + FloorDiv::Divide(8 * year + 21, int64_t(33));
	if (year > PERSIAN_MIN_CORRECTION && IsCorrectedYear(year - 1)) {
		julian_day--;
	}
	return julian_day;
}

int32_t PersianCalendar::HandleGetLimit(CalendarField field, LimitType type) const {
	return PERSIAN_LIMITS[field][int32_t(type)];
}

int64_t PersianCalendar::HandleComputeMonthStart(int32_t eyear, int32_t month, bool) const {
	// move a month outside of the year into range, adjusting the year with it
	if (month < 0 || month > 11) {
		const auto years = int64_t(eyear) + FloorDiv::Divide(month, 12, month);
		if (years < NumericLimits<int32_t>::Minimum() || years > NumericLimits<int32_t>::Maximum()) {
			Fail();
			return 0;
		}
		eyear = int32_t(years);
	}
	auto julian_day = PERSIAN_EPOCH - 1LL + FirstDayOfYear(eyear);
	if (month != 0) {
		julian_day += PERSIAN_DAYS_BEFORE[month];
	}
	return julian_day;
}

int32_t PersianCalendar::HandleGetMonthLength(int32_t eyear, int32_t month) const {
	if (month < 0 || month > 11) {
		eyear += FloorDiv::Divide(month, 12, month);
	}
	return IsPersianLeapYear(eyear) ? PERSIAN_LEAP_MONTH_LENGTH[month] : PERSIAN_MONTH_LENGTH[month];
}

int32_t PersianCalendar::HandleGetYearLength(int32_t eyear) const {
	return IsPersianLeapYear(eyear) ? 366 : 365;
}

int32_t PersianCalendar::HandleGetExtendedYear() {
	if (NewerField(CAL_EXTENDED_YEAR, CAL_YEAR) == CAL_EXTENDED_YEAR) {
		return InternalGet(CAL_EXTENDED_YEAR, 1);
	}
	return InternalGet(CAL_YEAR, 1);
}

void PersianCalendar::HandleComputeFields(int32_t julian_day) {
	const auto days_since_epoch = int64_t(julian_day) - PERSIAN_EPOCH;
	auto year = FloorDiv::Divide(33 * days_since_epoch + 3, int64_t(12053)) + 1;
	if (year > NumericLimits<int32_t>::Maximum() || year < NumericLimits<int32_t>::Minimum()) {
		Fail();
		return;
	}

	auto doy = int32_t(days_since_epoch - FirstDayOfYear(year));
	// a corrected year is one day shorter, so its last day already belongs to the next year
	if (doy == 365 && year >= PERSIAN_MIN_CORRECTION && IsCorrectedYear(year)) {
		year++;
		doy = 0;
	}

	// the first six months are 31 days long and the rest are 30
	const auto month = doy < 216 ? doy / 31 : (doy - 6) / 30;
	++doy;
	const auto dom = doy - PERSIAN_DAYS_BEFORE[month];

	InternalSet(CAL_ERA, 0);
	InternalSet(CAL_YEAR, int32_t(year));
	InternalSet(CAL_EXTENDED_YEAR, int32_t(year));
	InternalSet(CAL_MONTH, month);
	InternalSet(CAL_ORDINAL_MONTH, month);
	InternalSet(CAL_DATE, dom);
	InternalSet(CAL_DAY_OF_YEAR, doy);
}

//===--------------------------------------------------------------------===//
// Indian
//===--------------------------------------------------------------------===//
//! The Gregorian year that the Saka era starts in, and the day of that year it starts on
static constexpr int32_t INDIAN_ERA_START = 78;
static constexpr int32_t INDIAN_YEAR_START = 80;

static const int32_t INDIAN_LIMITS[CAL_FIELD_COUNT][4] = {
    //  minimum, greatest minimum, least maximum, maximum
    {0, 0, 0, 0},                           // ERA
    {-5000000, -5000000, 5000000, 5000000}, // YEAR
    {0, 0, 11, 11},                         // MONTH
    {1, 1, 52, 53},                         // WEEK_OF_YEAR
    {-1, -1, -1, -1},                       // WEEK_OF_MONTH
    {1, 1, 30, 31},                         // DATE
    {1, 1, 365, 366},                       // DAY_OF_YEAR
    {-1, -1, -1, -1},                       // DAY_OF_WEEK
    {-1, -1, 5, 5},                         // DAY_OF_WEEK_IN_MONTH
    {-1, -1, -1, -1},                       // AM_PM
    {-1, -1, -1, -1},                       // HOUR
    {-1, -1, -1, -1},                       // HOUR_OF_DAY
    {-1, -1, -1, -1},                       // MINUTE
    {-1, -1, -1, -1},                       // SECOND
    {-1, -1, -1, -1},                       // MILLISECOND
    {-1, -1, -1, -1},                       // ZONE_OFFSET
    {-1, -1, -1, -1},                       // DST_OFFSET
    {-5000000, -5000000, 5000000, 5000000}, // YEAR_WOY
    {-1, -1, -1, -1},                       // DOW_LOCAL
    {-5000000, -5000000, 5000000, 5000000}, // EXTENDED_YEAR
    {-1, -1, -1, -1},                       // JULIAN_DAY
    {-1, -1, -1, -1},                       // MILLISECONDS_IN_DAY
    {-1, -1, -1, -1},                       // IS_LEAP_MONTH
    {0, 0, 11, 11},                         // ORDINAL_MONTH
};

//! The Julian day of noon on a Gregorian date, which is what the Indian calendar is defined
//! relative to
static double GregorianToJulianDay(int32_t year, int32_t month, int32_t dom) {
	return double(Grego::FieldsToDay(year, month, dom)) + JULIAN_1970_CE - 0.5;
}

//! The Julian day of a 1-based Indian month and day in a Saka year
static double IndianToJulianDay(int32_t year, int32_t month, int32_t dom) {
	const auto gregorian_year = year + INDIAN_ERA_START;
	const auto leap = Grego::IsLeapYear(gregorian_year);
	// the year starts on the day after the vernal equinox
	const auto leap_month = leap ? 31 : 30;
	auto julian_day = GregorianToJulianDay(gregorian_year, 2, leap ? 21 : 22);

	if (month == 1) {
		return julian_day + (dom - 1);
	}
	julian_day += leap_month;
	julian_day += MinValue(month - 2, 5) * 31;
	if (month >= 8) {
		julian_day += (month - 7) * 30;
	}
	return julian_day + dom - 1;
}

int32_t IndianCalendar::HandleGetLimit(CalendarField field, LimitType type) const {
	return INDIAN_LIMITS[field][int32_t(type)];
}

int64_t IndianCalendar::HandleComputeMonthStart(int32_t eyear, int32_t month, bool) const {
	// move a month outside of the year into range, adjusting the year with it
	if (month < 0 || month > 11) {
		const auto years = int64_t(eyear) + FloorDiv::Divide(month, 12, month);
		if (years < NumericLimits<int32_t>::Minimum() || years > NumericLimits<int32_t>::Maximum()) {
			Fail();
			return 0;
		}
		eyear = int32_t(years);
	}
	return int64_t(IndianToJulianDay(eyear, month == 12 ? 1 : month + 1, 1));
}

int32_t IndianCalendar::HandleGetMonthLength(int32_t eyear, int32_t month) const {
	if (month < 0 || month > 11) {
		eyear += FloorDiv::Divide(month, 12, month);
	}
	if (Grego::IsLeapYear(eyear + INDIAN_ERA_START) && month == 0) {
		return 31;
	}
	if (month >= 1 && month <= 5) {
		return 31;
	}
	return 30;
}

int32_t IndianCalendar::HandleGetYearLength(int32_t eyear) const {
	return Grego::IsLeapYear(eyear + INDIAN_ERA_START) ? 366 : 365;
}

int32_t IndianCalendar::HandleGetExtendedYear() {
	if (NewerField(CAL_EXTENDED_YEAR, CAL_YEAR) == CAL_EXTENDED_YEAR) {
		return InternalGet(CAL_EXTENDED_YEAR, 1);
	}
	return InternalGet(CAL_YEAR, 1);
}

void IndianCalendar::HandleComputeFields(int32_t julian_day) {
	auto year = gregorian_year - INDIAN_ERA_START;
	// the day within the Gregorian year, 0-based
	auto doy = int32_t(julian_day - GregorianToJulianDay(gregorian_year, 0, 1));

	int32_t leap_month;
	if (doy < INDIAN_YEAR_START) {
		// the day is still in the Saka year that started in the previous Gregorian year
		year -= 1;
		leap_month = Grego::IsLeapYear(gregorian_year - 1) ? 31 : 30;
		doy += leap_month + (31 * 5) + (30 * 3) + 10;
	} else {
		leap_month = Grego::IsLeapYear(gregorian_year) ? 31 : 30;
		doy -= INDIAN_YEAR_START;
	}

	int32_t month;
	int32_t dom;
	if (doy < leap_month) {
		month = 0;
		dom = doy + 1;
	} else {
		auto day = doy - leap_month;
		if (day < 31 * 5) {
			month = day / 31 + 1;
			dom = (day % 31) + 1;
		} else {
			day -= 31 * 5;
			month = day / 30 + 6;
			dom = (day % 30) + 1;
		}
	}

	InternalSet(CAL_ERA, 0);
	InternalSet(CAL_EXTENDED_YEAR, year);
	InternalSet(CAL_YEAR, year);
	InternalSet(CAL_MONTH, month);
	InternalSet(CAL_ORDINAL_MONTH, month);
	InternalSet(CAL_DATE, dom);
	InternalSet(CAL_DAY_OF_YEAR, doy + 1);
}

} // namespace datetime
} // namespace duckdb
