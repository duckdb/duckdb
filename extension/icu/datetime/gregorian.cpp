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
		eyear += FloorDiv::Divide(month, 12, month);
	}
	return Grego::FieldsToDay(eyear, month, 0) + JULIAN_1970_CE;
}

int32_t GregorianCalendar::HandleGetMonthLength(int32_t eyear, int32_t month) const {
	if (month < 0 || month > 11) {
		eyear += FloorDiv::Divide(month, 12, month);
	}
	return Grego::MonthLength(eyear, month);
}

int32_t GregorianCalendar::HandleGetYearLength(int32_t eyear) const {
	return Grego::IsLeapYear(eyear) ? 366 : 365;
}

void GregorianCalendar::HandleComputeFields(int32_t julian_day) {
	// the Gregorian fields of the Julian day have already been computed
	auto eyear = gregorian_year;
	InternalSet(CAL_MONTH, gregorian_month);
	InternalSet(CAL_ORDINAL_MONTH, gregorian_month);
	InternalSet(CAL_DATE, gregorian_dom);
	InternalSet(CAL_DAY_OF_YEAR, gregorian_doy);
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

} // namespace datetime
} // namespace duckdb
