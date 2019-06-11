#include "common/types/timestamp.hpp"

#include "common/exception.hpp"
#include "common/types/date.hpp"
#include "common/types/time.hpp"

#include <iomanip>
#include <iostream>
#include <sstream>

using namespace duckdb;
using namespace std;

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
		str += DEFAULT_TIME;
	}
	// Character length	19 positions minimum to 23 maximum
	if (str.size() < STD_TIMESTAMP_LENGTH || str.size() > MAX_TIMESTAMP_LENGTH) {
		throw ConversionException("timestamp field value out of range: \"%s\", "
		                          "expected format is (YYYY-MM-DD hh:mm:ss)",
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
	return (dtime_t)timestamp;
}

timestamp_t Timestamp::FromDatetime(date_t date, dtime_t time) {
	return ((int64_t)date << 32 | (int32_t)time);
}

void Timestamp::Convert(timestamp_t date, date_t &out_date, dtime_t &out_time) {
	out_date = GetDate(date);
	out_time = GetTime(date);
}

timestamp_t Timestamp::GetCurrentTimestamp() {
	auto now = std::chrono::system_clock::now();
	auto in_time_t = std::chrono::system_clock::to_time_t(now);

	std::stringstream ss;
	ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X");
	return Timestamp::FromString(ss.str());
}

timestamp_t Timestamp::GetDifference(timestamp_t timestamp_1, timestamp_t timestamp_2) {
	// First extract the dates
	auto date1 = GetDate(timestamp_1);
	auto date2 = GetDate(timestamp_2);
	// and from date extract the years, months and days
	int32_t year1, month1, day1;
	int32_t year2, month2, day2;
	Date::Convert(date1, year1, month1, day1);
	Date::Convert(date2, year2, month2, day2);
	// finally perform the differences
	auto year_diff = year2 - year1;
	auto month_diff = month2 - month2;
	auto day_diff = day2 - day1;

	// Now we extract the time
	auto time1 = GetTime(timestamp_1);
	auto time2 = GetTime(timestamp_2);

	// and from time extract hours, minutes, seconds and miliseconds
	int32_t hour1, min1, sec1, msec1;
	int32_t hour2, min2, sec2, msec2;
	Time::Convert(time1, hour1, min1, sec1, msec1);
	Time::Convert(time2, hour2, min2, sec2, msec2);
	// finally perform the differences
	hour_diff = hour2 - hour1;
	min_diff = min2 - min1;
	sec_diff = sec2 - sec1;
	msec_diff = msec2 - msec1;

	// flip sign if necessary
	if (timestamp_2 < timestamp_1) {
		year_diff = -year_diff;
		month_diff = -month_diff;
		day_diff = -day_diff;
		hour_diff = -hour_diff;
		min_diff = -min_diff;
		sec_diff = -sec_diff;
		msec_diff = -msec_diff;
	}
	// now propagate any negative field into the next higher field

	auto date_diff = date_b - date_a;
	auto time_diff = time_b - time_a;
	return FromDatetime(date_diff, time_diff);
}