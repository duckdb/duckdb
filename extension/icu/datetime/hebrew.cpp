#include "hebrew.hpp"

#include "grego.hpp"

namespace duckdb {
namespace datetime {

static const int32_t HEBREW_LIMITS[CAL_FIELD_COUNT][4] = {
    //  minimum, greatest minimum, least maximum, maximum
    {0, 0, 0, 0},                           // ERA
    {-5000000, -5000000, 5000000, 5000000}, // YEAR
    {0, 0, 12, 12},                         // MONTH
    {1, 1, 51, 56},                         // WEEK_OF_YEAR
    {-1, -1, -1, -1},                       // WEEK_OF_MONTH
    {1, 1, 29, 30},                         // DATE
    {1, 1, 353, 385},                       // DAY_OF_YEAR
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
    {0, 0, 11, 12},                         // ORDINAL_MONTH
};

//! The length of every month in a deficient, a normal and a complete year
static const int8_t HEBREW_MONTH_LENGTH[][3] = {
    {30, 30, 30}, // Tishri
    {29, 29, 30}, // Heshvan
    {29, 30, 30}, // Kislev
    {29, 29, 29}, // Tevet
    {30, 30, 30}, // Shevat
    {30, 30, 30}, // Adar I, in a leap year only
    {29, 29, 29}, // Adar
    {30, 30, 30}, // Nisan
    {29, 29, 29}, // Iyar
    {30, 30, 30}, // Sivan
    {29, 29, 29}, // Tammuz
    {30, 30, 30}, // Av
    {29, 29, 29}, // Elul
};

//! The day every month ends on in a year without a leap month
static const int16_t HEBREW_MONTH_START[][3] = {
    {0, 0, 0},       // (the day before the year starts)
    {30, 30, 30},    // Tishri
    {59, 59, 60},    // Heshvan
    {88, 89, 90},    // Kislev
    {117, 118, 119}, // Tevet
    {147, 148, 149}, // Shevat
    {147, 148, 149}, // (Adar I, which is absent)
    {176, 177, 178}, // Adar
    {206, 207, 208}, // Nisan
    {235, 236, 237}, // Iyar
    {265, 266, 267}, // Sivan
    {294, 295, 296}, // Tammuz
    {324, 325, 326}, // Av
    {353, 354, 355}, // Elul
};

//! The day every month ends on in a year with a leap month
static const int16_t HEBREW_LEAP_MONTH_START[][3] = {
    {0, 0, 0},       // (the day before the year starts)
    {30, 30, 30},    // Tishri
    {59, 59, 60},    // Heshvan
    {88, 89, 90},    // Kislev
    {117, 118, 119}, // Tevet
    {147, 148, 149}, // Shevat
    {177, 178, 179}, // Adar I
    {206, 207, 208}, // Adar II
    {236, 237, 238}, // Nisan
    {265, 266, 267}, // Iyar
    {295, 296, 297}, // Sivan
    {324, 325, 326}, // Tammuz
    {354, 355, 356}, // Av
    {383, 384, 385}, // Elul
};

//! The dates are computed in days, hours and parts, where a part is 1/1080 of an hour
static constexpr int32_t HOUR_PARTS = 1080;
static constexpr int32_t DAY_PARTS = 24 * HOUR_PARTS;
//! The length of a lunar month
static constexpr int32_t MONTH_DAYS = 29;
static constexpr int32_t MONTH_FRACT = 12 * HOUR_PARTS + 793;
static constexpr int32_t MONTH_PARTS = MONTH_DAYS * DAY_PARTS + MONTH_FRACT;
//! The new moon on the first of Tishri of year 1, counted from noon on the day before
static constexpr int32_t BAHARAD = 11 * HOUR_PARTS + 204;
//! The Julian day that day zero of the calendar falls on
static constexpr int32_t HEBREW_EPOCH = 347997;

bool HebrewCalendar::IsLeapYear(int32_t year) {
	const auto x = (year * 12LL + 17) % YEARS_IN_CYCLE;
	return x >= ((x < 0) ? -7 : 12);
}

static int32_t MonthsInYear(int32_t year) {
	return HebrewCalendar::IsLeapYear(year) ? 13 : 12;
}

//! The day a year starts on, counted from the epoch. The festivals may not fall on certain days
//! of the week, so the start of the year is postponed to avoid them.
static int64_t StartOfYear(int32_t year, bool &failed) {
	const auto months = FloorDiv::Divide(235 * int64_t(year) - 234, int64_t(19));
	auto fraction = months * MONTH_FRACT + BAHARAD;
	auto day = months * 29LL + fraction / DAY_PARTS;
	fraction = fraction % DAY_PARTS;

	auto weekday = int32_t(day % 7);
	if (weekday == 2 || weekday == 4 || weekday == 6) {
		// the first may not fall on a Sunday, a Wednesday or a Friday
		day += 1;
		weekday = int32_t(day % 7);
	} else if (weekday == 1 && fraction > 15 * HOUR_PARTS + 204 && !HebrewCalendar::IsLeapYear(year)) {
		// a new moon after 3:11:20am on a Tuesday in a year without a leap month is postponed
		// by two days, which prevents a year of 356 days
		day += 2;
	} else if (weekday == 0 && fraction > 21 * HOUR_PARTS + 589 && HebrewCalendar::IsLeapYear(year - 1)) {
		// a new moon after 9:32:43am on a Monday following a year with a leap month is postponed
		// by a day, which prevents a year of 382 days
		day += 1;
	}

	if (day > NumericLimits<int32_t>::Maximum() || day < NumericLimits<int32_t>::Minimum()) {
		failed = true;
		return 0;
	}
	return day;
}

//! Whether the year is deficient (0), normal (1) or complete (2), which is what the length of
//! Heshvan and Kislev depends on
static int32_t YearType(int32_t year, bool &failed) {
	auto length = int32_t(StartOfYear(year + 1, failed) - StartOfYear(year, failed));
	if (length > 380) {
		// a leap month does not change the type
		length -= 30;
	}
	switch (length) {
	case 353:
		return 0;
	case 355:
		return 2;
	default:
		return 1;
	}
}

int32_t HebrewCalendar::HandleGetLimit(CalendarField field, LimitType type) const {
	return HEBREW_LIMITS[field][int32_t(type)];
}

void HebrewCalendar::AddChecked(CalendarField field, int32_t amount) {
	if (field != CAL_MONTH && field != CAL_ORDINAL_MONTH) {
		FieldCalendar::AddChecked(field, amount);
		return;
	}
	if (HasFailedInternal() || amount == 0) {
		return;
	}

	// The months are always numbered 0..12, and the leap month is simply absent from a year that
	// does not have one, so adding months has to step over it in the direction it is going.
	int64_t month = GetChecked(CAL_MONTH);
	auto year = GetChecked(CAL_YEAR);
	if (HasFailedInternal()) {
		return;
	}

	if (amount > 0) {
		auto across_adar_1 = month < ADAR_1;
		month += amount;
		// fast forward over the whole cycles first, which all hold the same number of months
		if (month >= MONTHS_IN_CYCLE) {
			int32_t years;
			if (!TryAdd(year, int32_t(month / MONTHS_IN_CYCLE) * YEARS_IN_CYCLE, years)) {
				Fail();
				return;
			}
			year = years;
			month %= MONTHS_IN_CYCLE;
		}
		for (;;) {
			if (across_adar_1 && month >= ADAR_1 && !IsLeapYear(year)) {
				++month;
			}
			if (month <= ELUL) {
				break;
			}
			month -= ELUL + 1;
			++year;
			across_adar_1 = true;
		}
	} else {
		auto across_adar_1 = month > ADAR_1;
		month += amount;
		if (month <= -MONTHS_IN_CYCLE) {
			int32_t years;
			if (!TryAdd(year, int32_t(month / MONTHS_IN_CYCLE) * YEARS_IN_CYCLE, years)) {
				Fail();
				return;
			}
			year = years;
			month %= MONTHS_IN_CYCLE;
		}
		for (;;) {
			if (across_adar_1 && month <= ADAR_1 && !IsLeapYear(year)) {
				--month;
			}
			if (month >= 0) {
				break;
			}
			month += ELUL + 1;
			--year;
			across_adar_1 = true;
		}
	}

	Set(CAL_MONTH, int32_t(month));
	Set(CAL_YEAR, year);
	PinField(CAL_DATE);
}

int64_t HebrewCalendar::HandleComputeMonthStart(int32_t eyear, int32_t month, bool) const {
	// The months are numbered 0..12 in every year, so a month outside of that range is moved
	// into it a whole year at a time - which is twelve or thirteen months.
	if (month <= -MONTHS_IN_CYCLE || month >= MONTHS_IN_CYCLE) {
		int32_t years;
		if (!TryAdd(eyear, (month / MONTHS_IN_CYCLE) * YEARS_IN_CYCLE, years)) {
			Fail();
			return 0;
		}
		eyear = years;
		month %= MONTHS_IN_CYCLE;
	}
	while (month < 0) {
		if (!TryAdd(eyear, -1, eyear) || !TryAdd(month, MonthsInYear(eyear), month)) {
			Fail();
			return 0;
		}
	}
	while (month > 12) {
		if (!TryAdd(month, -MonthsInYear(eyear), month) || !TryAdd(eyear, 1, eyear)) {
			Fail();
			return 0;
		}
	}

	auto failure = false;
	auto day = StartOfYear(eyear, failure);
	if (month != 0) {
		const auto type = YearType(eyear, failure);
		day += IsLeapYear(eyear) ? HEBREW_LEAP_MONTH_START[month][type] : HEBREW_MONTH_START[month][type];
	}
	if (failure) {
		Fail();
		return 0;
	}
	return day + HEBREW_EPOCH;
}

int32_t HebrewCalendar::HandleGetMonthLength(int32_t eyear, int32_t month) const {
	// a month outside of the year is moved into it a whole year at a time, which is twelve or
	// thirteen months
	while (month < 0) {
		month += MonthsInYear(--eyear);
	}
	while (month > 12) {
		month -= MonthsInYear(eyear++);
	}

	if (month == HESHVAN || month == KISLEV) {
		// only these two vary with the length of the year
		auto failure = false;
		const auto type = YearType(eyear, failure);
		if (failure) {
			Fail();
			return 0;
		}
		return HEBREW_MONTH_LENGTH[month][type];
	}
	return HEBREW_MONTH_LENGTH[month][0];
}

int32_t HebrewCalendar::HandleGetYearLength(int32_t eyear) const {
	auto failure = false;
	const auto length = int32_t(StartOfYear(eyear + 1, failure) - StartOfYear(eyear, failure));
	if (failure) {
		Fail();
		return 0;
	}
	return length;
}

int32_t HebrewCalendar::HandleGetExtendedYear() {
	if (NewerField(CAL_EXTENDED_YEAR, CAL_YEAR) == CAL_EXTENDED_YEAR) {
		return InternalGet(CAL_EXTENDED_YEAR, 1);
	}
	return InternalGet(CAL_YEAR, 1);
}

void HebrewCalendar::HandleComputeFields(int32_t julian_day) {
	const auto days = int64_t(julian_day) - HEBREW_EPOCH;
	if (days > NumericLimits<int32_t>::Maximum() || days < NumericLimits<int32_t>::Minimum()) {
		Fail();
		return;
	}

	// estimate the year from the number of lunar months that have passed. The parts of a day are
	// whole numbers, so the estimate is exact rather than the nearest a double can hold.
	const auto months = FloorDiv::Divide(days * int64_t(DAY_PARTS), int64_t(MONTH_PARTS));
	auto year = int32_t(FloorDiv::Divide(19 * months + 234, int64_t(235)) + 1);

	auto failure = false;
	auto year_start = StartOfYear(year, failure);
	auto doy = int32_t(days - year_start);
	// the postponement rules make the estimate too high every so often
	while (!failure && doy < 1) {
		year--;
		year_start = StartOfYear(year, failure);
		doy = int32_t(days - year_start);
	}
	const auto type = YearType(year, failure);
	if (failure) {
		Fail();
		return;
	}

	const auto is_leap = IsLeapYear(year);
	const auto &month_start = is_leap ? HEBREW_LEAP_MONTH_START : HEBREW_MONTH_START;
	static constexpr int32_t MONTH_COUNT = 14;
	int32_t month = 0;
	while (month < MONTH_COUNT && doy > month_start[month][type]) {
		month++;
	}
	if (month >= MONTH_COUNT || month <= 0) {
		// the day is outside of the range that the year can hold, which happens for the
		// extremes of the representable range
		Fail();
		return;
	}
	month--;
	const auto dom = doy - month_start[month][type];

	InternalSet(CAL_ERA, 0);
	year = MinValue(MaxValue(year, HandleGetLimit(CAL_EXTENDED_YEAR, LimitType::MINIMUM)),
	                HandleGetLimit(CAL_EXTENDED_YEAR, LimitType::MAXIMUM));
	InternalSet(CAL_YEAR, year);
	InternalSet(CAL_EXTENDED_YEAR, year);
	// the ordinal month skips the leap month in the years that do not have one
	InternalSet(CAL_ORDINAL_MONTH, (!is_leap && month > ADAR_1) ? month - 1 : month);
	InternalSet(CAL_MONTH, month);
	InternalSet(CAL_DATE, dom);
	InternalSet(CAL_DAY_OF_YEAR, doy);
}

} // namespace datetime
} // namespace duckdb
