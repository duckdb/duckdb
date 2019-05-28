#include "common/types/timestamp.hpp"

#include "common/exception.hpp"
#include "common/types/date.hpp"
#include "common/types/time.hpp"

#include <iomanip>
#include <iostream>
#include <sstream>

using namespace duckdb;
using namespace std;

constexpr const size_t STD_TIMESTAMP_LENGTH = 19;

// timestamp/datetime uses 64 bits, high 32 bits for date and low 32 bits for time
// string format is YYYY-MM-DDThh:mm:ssZ
// T may be a space
// Z is optional
// ISO 8601

timestamp_t Timestamp::FromString(string str) {
	assert(sizeof(timestamp_t) == 8);
	assert(sizeof(date_t) == 4);
	assert(sizeof(dtime_t) == 4);

	// Character length	19 positions minimum to 23 maximum
	if (str.size() < STD_TIMESTAMP_LENGTH) {
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

	return Date::ToString(GetDate(timestamp)) + "T" + Time::ToString(GetTime(timestamp)) + "Z";
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