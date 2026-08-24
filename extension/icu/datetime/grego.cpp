#include "grego.hpp"

namespace duckdb {
namespace datetime {

const int16_t Grego::DAYS_BEFORE[24] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334,
                                        0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335};

const int8_t Grego::MONTH_LENGTH[24] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
                                        31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

int64_t Grego::FieldsToDay(int32_t year, int32_t month, int32_t dom) {
	int64_t y = year;
	y--;

	// the Julian calendar day, corrected to the Gregorian calendar and the month/day within the year
	const int64_t julian = 365 * y + FloorDiv::Divide(y, int64_t(4)) + (JULIAN_1_CE - 3) +
	                       FloorDiv::Divide(y, int64_t(400)) - FloorDiv::Divide(y, int64_t(100)) + 2 +
	                       DaysBeforeMonth(year, month) + dom;

	return julian - JULIAN_1970_CE;
}

int32_t Grego::DayToYear(int32_t day, int16_t &doy) {
	// convert from the 1970 CE epoch to the 1 CE epoch
	day += JULIAN_1970_CE - JULIAN_1_CE;

	// convert the day number to a multiple radix representation using the 400, 100 and 4 year cycles
	int32_t remainder;
	const auto n400 = FloorDiv::Divide(day, 146097, remainder);
	const auto n100 = FloorDiv::Divide(remainder, 36524, remainder);
	const auto n4 = FloorDiv::Divide(remainder, 1461, remainder);
	const auto n1 = FloorDiv::Divide(remainder, 365, remainder);
	auto year = 400 * n400 + 100 * n100 + 4 * n4 + n1;
	if (n100 == 4 || n1 == 4) {
		// December 31 at the end of a 4 or 400 year cycle
		doy = 365;
	} else {
		doy = static_cast<int16_t>(remainder);
		++year;
	}
	++doy;
	return year;
}

void Grego::DayToFields(int32_t day, int32_t &year, int8_t &month, int8_t &dom, int8_t &dow, int16_t &doy) {
	year = DayToYear(day, doy);

	// convert from the 1970 CE epoch to the 1 CE epoch - Gregorian day zero is a Monday
	day += JULIAN_1970_CE - JULIAN_1_CE;
	auto weekday = (day + 1) % 7;
	weekday += (weekday < 0) ? 8 : 1;
	dow = static_cast<int8_t>(weekday);

	const auto is_leap = IsLeapYear(year);
	const int32_t march1 = is_leap ? 60 : 59;
	int32_t correction = 0;
	if (doy > march1) {
		correction = is_leap ? 1 : 2;
	}
	month = static_cast<int8_t>((12 * (doy - 1 + correction) + 6) / 367);
	dom = static_cast<int8_t>(doy - DaysBeforeMonth(year, month));
}

bool Grego::TimeToFields(int64_t time, int32_t &year, int8_t &month, int8_t &dom, int8_t &dow, int16_t &doy,
                         int32_t &mid) {
	const auto day = FloorDiv::Divide(time, static_cast<int32_t>(MILLIS_PER_DAY), mid);
	if (day > int64_t(NumericLimits<int32_t>::Maximum()) || day < int64_t(NumericLimits<int32_t>::Minimum())) {
		return false;
	}
	DayToFields(static_cast<int32_t>(day), year, month, dom, dow, doy);
	return true;
}

int32_t Grego::DayOfWeek(int32_t day) {
	int32_t dow;
	// day zero of the 1970 epoch is a Thursday
	FloorDiv::Divide(day + 5, 7, dow);
	return (dow == 0) ? 7 : dow;
}

int32_t Grego::DayOfWeekInMonth(int32_t year, int32_t month, int32_t dom) {
	auto week_in_month = (dom + 6) / 7;
	if (week_in_month == 4) {
		if (dom + 7 > MonthLength(year, month)) {
			week_in_month = -1;
		}
	} else if (week_in_month == 5) {
		week_in_month = -1;
	}
	return week_in_month;
}

} // namespace datetime
} // namespace duckdb
