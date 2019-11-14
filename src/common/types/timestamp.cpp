#include "duckdb/common/types/timestamp.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"

#include <iomanip>
#include <iostream>
#include <sstream>

using namespace duckdb;
using namespace std;

constexpr const int32_t MONTHS_PER_YEAR = 12;
constexpr const int32_t HOURS_PER_DAY = 24; //! assume no daylight savings time changes
constexpr const int32_t STD_TIMESTAMP_LENGTH = 19;
constexpr const int32_t START_YEAR = 1900;

constexpr const int32_t SECS_PER_MINUTE = 60;
constexpr const int32_t MINS_PER_HOUR = 60;
constexpr const int64_t MSECS_PER_HOUR = 360000;
constexpr const int64_t MSECS_PER_MINUTE = 60000;
constexpr const int64_t MSECS_PER_SEC = 1000;

// Used to check amount of days per month in common year and leap year
constexpr int days_per_month[2][13] = {{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0},
                                       {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0}};
constexpr bool isleap(int16_t year) {
	return (((year) % 4) == 0 && (((year) % 100) != 0 || ((year) % 400) == 0));
}

// timestamp/datetime uses 64 bits, high 32 bits for date and low 32 bits for time
// string format is YYYY-MM-DDThh:mm:ssZ
// T may be a space
// Z is optional
// ISO 8601

timestamp_t Timestamp::FromString(string str) {
	assert(sizeof(timestamp_t) == 8);
	assert(sizeof(date_t) == 4);
	assert(sizeof(dtime_t) == 4);

	// In case we have only date we add a default time
	if (str.size() == 10) {
		str += " 00:00:00";
	}
	// Character length	19 positions minimum to 23 maximum
	if (str.size() < STD_TIMESTAMP_LENGTH) {
		throw ConversionException("timestamp field value out of range: \"%s\", "
		                          "expected format is (YYYY-MM-DD HH:MM:SS[.MS])",
		                          str.c_str());
	}

	date_t date = Date::FromString(str.substr(0, 10));
	dtime_t time = Time::FromString(str.substr(10));

	return ((int64_t)date << 32 | (int32_t)time);
}

string Timestamp::ToString(timestamp_t timestamp) {
	assert(sizeof(timestamp_t) == 8);
	assert(sizeof(date_t) == 4);
	assert(sizeof(dtime_t) == 4);

	return Date::ToString(GetDate(timestamp)) + " " + Time::ToString(GetTime(timestamp));
}

date_t Timestamp::GetDate(timestamp_t timestamp) {
	return (date_t)(((int64_t)timestamp) >> 32);
}

dtime_t Timestamp::GetTime(timestamp_t timestamp) {
	return (dtime_t)(timestamp & 0xFFFFFFFF);
}

timestamp_t Timestamp::FromDatetime(date_t date, dtime_t time) {
	return ((int64_t)date << 32 | (int64_t)time);
}

void Timestamp::Convert(timestamp_t date, date_t &out_date, dtime_t &out_time) {
	out_date = GetDate(date);
	out_time = GetTime(date);
}

timestamp_t Timestamp::GetCurrentTimestamp() {
	auto in_time_t = std::time(nullptr);
	auto utc = std::gmtime(&in_time_t);

	// tm_year[0...] considers the amount of years since 1900 and tm_mon considers the amount of months since january
	// tm_mon[0-11]
	auto date = Date::FromDate(utc->tm_year + START_YEAR, utc->tm_mon + 1, utc->tm_mday);
	auto time = Time::FromTime(utc->tm_hour, utc->tm_min, utc->tm_sec);

	return Timestamp::FromDatetime(date, time);
}

Interval Timestamp::GetDifference(timestamp_t timestamp_1, timestamp_t timestamp_2) {
	// First extract the dates
	auto date1 = GetDate(timestamp_1);
	auto date2 = GetDate(timestamp_2);
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
	auto time1 = GetTime(timestamp_1);
	auto time2 = GetTime(timestamp_2);

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
	Interval interval;
	interval.months = year_diff * MONTHS_PER_YEAR + month_diff;
	interval.days = day_diff;
	interval.time =
	    ((((((hour_diff * MINS_PER_HOUR) + min_diff) * SECS_PER_MINUTE) + sec_diff) * MSECS_PER_SEC) + msec_diff);

	return interval;
}

timestamp_struct Timestamp::IntervalToTimestamp(Interval &interval) {
	timestamp_struct timestamp;

	if (interval.months != 0) {
		timestamp.year = interval.months / MONTHS_PER_YEAR;
		timestamp.month = interval.months % MONTHS_PER_YEAR;

	} else {
		timestamp.year = 0;
		timestamp.month = 0;
	}
	timestamp.day = interval.days;
	auto time = interval.time;

	timestamp.hour = time / MSECS_PER_HOUR;
	time -= timestamp.hour * MSECS_PER_HOUR;
	timestamp.min = time / MSECS_PER_MINUTE;
	time -= timestamp.min * MSECS_PER_MINUTE;
	timestamp.sec = time / MSECS_PER_SEC;
	timestamp.msec = time - (timestamp.sec * MSECS_PER_SEC);
	return timestamp;
}

Interval TimestampToInterval(timestamp_struct *timestamp) {
	Interval interval;

	interval.months = timestamp->year * MONTHS_PER_YEAR + timestamp->month;
	interval.days = timestamp->day;
	interval.time =
	    ((((((timestamp->hour * MINS_PER_HOUR) + timestamp->min) * SECS_PER_MINUTE) + timestamp->sec) * MSECS_PER_SEC) +
	     timestamp->msec);

	return interval;
}

int64_t Timestamp::GetEpoch(timestamp_t timestamp) {
	return Date::Epoch(Timestamp::GetDate(timestamp)) + (int64_t)(Timestamp::GetTime(timestamp) / 1000);
}

int64_t Timestamp::GetMilliseconds(timestamp_t timestamp) {
	int n = Timestamp::GetTime(timestamp);
	int m = n / 60000;
	return n - m * 60000;
}

int64_t Timestamp::GetSeconds(timestamp_t timestamp) {
	int n = Timestamp::GetTime(timestamp);
	int m = n / 60000;
	return (n - m * 60000) / 1000;
}

int64_t Timestamp::GetMinutes(timestamp_t timestamp) {
	int n = Timestamp::GetTime(timestamp);
	int h = n / 3600000;
	return (n - h * 3600000) / 60000;
}

int64_t Timestamp::GetHours(timestamp_t timestamp) {
	return Timestamp::GetTime(timestamp) / 3600000;
}
