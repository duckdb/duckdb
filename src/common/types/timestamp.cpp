#include "duckdb/common/types/timestamp.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"

#include <iomanip>
#include <iostream>
#include <sstream>

using namespace duckdb;
using namespace std;

constexpr const int32_t STD_TIMESTAMP_LENGTH = 19;
constexpr const int32_t TM_START_YEAR = 1900;

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
	auto date = Date::FromDate(utc->tm_year + TM_START_YEAR, utc->tm_mon + 1, utc->tm_mday);
	auto time = Time::FromTime(utc->tm_hour, utc->tm_min, utc->tm_sec);

	return Timestamp::FromDatetime(date, time);
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
