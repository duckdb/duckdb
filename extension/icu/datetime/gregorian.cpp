#include "gregorian.hpp"

#include "grego.hpp"

namespace duckdb {
namespace datetime {

//! The limits of the Gregorian calendar fields. The year limits follow from the range of Julian
//! days that can be represented as milliseconds.
static const int32_t GREGORIAN_LIMITS[CAL_FIELD_COUNT][4] = {
    //  minimum, greatest minimum, least maximum, maximum
    {0, 0, 1, 1},                       // ERA
    {1, 1, 140742, 144683},             // YEAR
    {0, 0, 11, 11},                     // MONTH
    {1, 1, 52, 53},                     // WEEK_OF_YEAR
    {-1, -1, -1, -1},                   // WEEK_OF_MONTH
    {1, 1, 28, 31},                     // DATE
    {1, 1, 365, 366},                   // DAY_OF_YEAR
    {-1, -1, -1, -1},                   // DAY_OF_WEEK
    {-1, -1, 4, 5},                     // DAY_OF_WEEK_IN_MONTH
    {-1, -1, -1, -1},                   // AM_PM
    {-1, -1, -1, -1},                   // HOUR
    {-1, -1, -1, -1},                   // HOUR_OF_DAY
    {-1, -1, -1, -1},                   // MINUTE
    {-1, -1, -1, -1},                   // SECOND
    {-1, -1, -1, -1},                   // MILLISECOND
    {-1, -1, -1, -1},                   // ZONE_OFFSET
    {-1, -1, -1, -1},                   // DST_OFFSET
    {-140742, -140742, 140742, 144683}, // YEAR_WOY
    {-1, -1, -1, -1},                   // DOW_LOCAL
    {-140742, -140742, 140742, 144683}, // EXTENDED_YEAR
    {-1, -1, -1, -1},                   // JULIAN_DAY
    {-1, -1, -1, -1},                   // MILLISECONDS_IN_DAY
    {-1, -1, -1, -1},                   // IS_LEAP_MONTH
    {0, 0, 11, 11},                     // ORDINAL_MONTH
};

int32_t GregorianCalendar::HandleGetLimit(CalendarField field, LimitType type) const {
	return GREGORIAN_LIMITS[field][int32_t(type)];
}

int64_t GregorianCalendar::HandleComputeMonthStart(int32_t eyear, int32_t month, bool) const {
	// move a month outside of the year into range, adjusting the year with it
	if (month < 0 || month > 11) {
		const auto years = int64_t(eyear) + FloorDiv::Divide(month, 12, month);
		if (years < NumericLimits<int32_t>::Minimum() || years > NumericLimits<int32_t>::Maximum()) {
			Fail();
			return 0;
		}
		eyear = int32_t(years);
	}

	auto is_leap = eyear % 4 == 0;
	const int64_t y = int64_t(eyear) - 1;
	// the Julian calendar day of the day before the first of January
	int64_t julian_day = 365 * y + FloorDiv::Divide(y, int64_t(4)) + JULIAN_1_CE - 3;

	is_gregorian = (eyear >= cutover_year);
	if (invert_gregorian) {
		is_gregorian = !is_gregorian;
	}
	if (is_gregorian) {
		is_leap = is_leap && ((eyear % 100 != 0) || (eyear % 400 == 0));
		julian_day += Grego::GregorianShift(eyear);
	}

	if (month != 0) {
		julian_day += Grego::DaysBeforeMonth(month, is_leap);
	}
	return julian_day;
}

int32_t GregorianCalendar::HandleGetMonthLength(int32_t eyear, int32_t month) const {
	if (month < 0 || month > 11) {
		eyear += FloorDiv::Divide(month, 12, month);
	}
	return Grego::MonthLength(month, IsLeapYear(eyear));
}

int32_t GregorianCalendar::HandleGetYearLength(int32_t eyear) const {
	return IsLeapYear(eyear) ? 366 : 365;
}

int32_t GregorianCalendar::HandleComputeJulianDay(CalendarField best_field) {
	invert_gregorian = false;

	auto julian_day = FieldCalendar::HandleComputeJulianDay(best_field);
	// weeks of the cutover year are counted relative to the Julian first of January
	if (best_field == CAL_WEEK_OF_YEAR && InternalGet(CAL_EXTENDED_YEAR) == cutover_year &&
	    julian_day >= cutover_julian_day) {
		invert_gregorian = true;
		return FieldCalendar::HandleComputeJulianDay(best_field);
	}

	// the fields can describe a date on the other side of the cutover than the one that was
	// assumed while resolving them, in which case they are resolved again
	if (is_gregorian != (julian_day >= cutover_julian_day)) {
		invert_gregorian = true;
		julian_day = FieldCalendar::HandleComputeJulianDay(best_field);
	}

	if (is_gregorian && InternalGet(CAL_EXTENDED_YEAR) == cutover_year) {
		// the days that the cutover skipped are not part of the year
		if (best_field == CAL_DAY_OF_YEAR) {
			julian_day -= Grego::GregorianShift(InternalGet(CAL_EXTENDED_YEAR));
		} else if (best_field == CAL_WEEK_OF_MONTH) {
			julian_day += 14;
		}
	}
	return julian_day;
}

void GregorianCalendar::HandleComputeFields(int32_t julian_day) {
	int32_t eyear;
	int32_t month;
	int32_t dom;
	int32_t doy;
	if (julian_day >= cutover_julian_day) {
		// the Gregorian fields of the Julian day have already been computed
		eyear = gregorian_year;
		month = gregorian_month;
		dom = gregorian_dom;
		doy = gregorian_doy;
	} else {
		// the Julian calendar epoch day is zero on Saturday December 30, 0 (Gregorian)
		const auto julian_epoch_day = julian_day - (JULIAN_1_CE - 2);
		int32_t unused;
		eyear = int32_t(FloorDiv::Divide(4 * int64_t(julian_epoch_day) + 1464, int32_t(1461), unused));

		const auto january1 = 365 * (eyear - 1) + FloorDiv::Divide(eyear - 1, 4);
		doy = julian_epoch_day - january1;

		// Julian leap years are every four years throughout, which is the proleptic rule
		const auto is_leap = (eyear & 0x3) == 0;
		const int32_t march1 = is_leap ? 60 : 59;
		int32_t correction = 0;
		if (doy >= march1) {
			correction = is_leap ? 1 : 2;
		}
		month = (12 * (doy + correction) + 6) / 367;
		dom = doy - Grego::DaysBeforeMonth(month, is_leap) + 1;
		++doy;
	}

	// after the cutover in its own year, the days that the cutover skipped are not counted
	if (eyear == cutover_year && julian_day >= cutover_julian_day) {
		doy += Grego::GregorianShift(eyear);
	}

	InternalSet(CAL_MONTH, month);
	InternalSet(CAL_ORDINAL_MONTH, month);
	InternalSet(CAL_DATE, dom);
	InternalSet(CAL_DAY_OF_YEAR, doy);
	InternalSet(CAL_EXTENDED_YEAR, eyear);

	auto era = AD;
	if (eyear < 1) {
		era = BC;
		eyear = 1 - eyear;
	}
	InternalSet(CAL_ERA, era);
	InternalSet(CAL_YEAR, eyear);
}

int32_t GregorianCalendar::HandleGetExtendedYear() {
	// there are three fields that the year can be derived from - use the most recently set one
	const auto year_field = NewerField(NewerField(CAL_EXTENDED_YEAR, CAL_YEAR), CAL_YEAR_WOY);
	switch (year_field) {
	case CAL_EXTENDED_YEAR:
		return InternalGet(CAL_EXTENDED_YEAR, EPOCH_YEAR);
	case CAL_YEAR: {
		// the year defaults to the start of the epoch, and the era to AD
		const auto era = InternalGet(CAL_ERA, AD);
		if (era == BC) {
			return 1 - InternalGet(CAL_YEAR, 1);
		}
		return InternalGet(CAL_YEAR, EPOCH_YEAR);
	}
	case CAL_YEAR_WOY: {
		auto year_woy = InternalGet(CAL_YEAR_WOY);
		if (InternalGet(CAL_ERA, AD) == BC) {
			year_woy = 1 - year_woy;
		}
		return HandleGetExtendedYearFromWeekFields(year_woy, InternalGet(CAL_WEEK_OF_YEAR));
	}
	default:
		return EPOCH_YEAR;
	}
}

//===--------------------------------------------------------------------===//
// Buddhist
//===--------------------------------------------------------------------===//
void BuddhistCalendar::HandleComputeFields(int32_t julian_day) {
	GregorianCalendar::HandleComputeFields(julian_day);
	InternalSet(CAL_ERA, BE);
	InternalSet(CAL_YEAR, InternalGet(CAL_EXTENDED_YEAR) - ERA_START);
}

int32_t BuddhistCalendar::HandleGetExtendedYear() {
	// the extended year is a Gregorian year, defaulting to the start of the epoch
	if (NewerField(CAL_EXTENDED_YEAR, CAL_YEAR) == CAL_EXTENDED_YEAR) {
		return InternalGet(CAL_EXTENDED_YEAR, EPOCH_YEAR);
	}
	return InternalGet(CAL_YEAR, EPOCH_YEAR - ERA_START) + ERA_START;
}

int32_t BuddhistCalendar::HandleGetLimit(CalendarField field, LimitType type) const {
	if (field == CAL_ERA) {
		return BE;
	}
	return GregorianCalendar::HandleGetLimit(field, type);
}

//===--------------------------------------------------------------------===//
// Republic of China
//===--------------------------------------------------------------------===//
void ROCCalendar::HandleComputeFields(int32_t julian_day) {
	GregorianCalendar::HandleComputeFields(julian_day);
	const auto year = InternalGet(CAL_EXTENDED_YEAR) - ERA_START;
	if (year > 0) {
		InternalSet(CAL_ERA, MINGUO);
		InternalSet(CAL_YEAR, year);
	} else {
		InternalSet(CAL_ERA, BEFORE_MINGUO);
		InternalSet(CAL_YEAR, 1 - year);
	}
}

int32_t ROCCalendar::HandleGetExtendedYear() {
	// the extended year is a Gregorian year, defaulting to the start of the epoch
	if (NewerField(CAL_EXTENDED_YEAR, CAL_YEAR) == CAL_EXTENDED_YEAR &&
	    NewerField(CAL_EXTENDED_YEAR, CAL_ERA) == CAL_EXTENDED_YEAR) {
		return InternalGet(CAL_EXTENDED_YEAR, EPOCH_YEAR);
	}
	const auto year = InternalGet(CAL_YEAR, 1);
	if (InternalGet(CAL_ERA, MINGUO) == BEFORE_MINGUO) {
		return 1 + ERA_START - year;
	}
	return year + ERA_START;
}

int32_t ROCCalendar::HandleGetLimit(CalendarField field, LimitType type) const {
	if (field != CAL_ERA) {
		return GregorianCalendar::HandleGetLimit(field, type);
	}
	if (type == LimitType::MINIMUM || type == LimitType::GREATEST_MINIMUM) {
		return BEFORE_MINGUO;
	}
	return MINGUO;
}

} // namespace datetime
} // namespace duckdb
