#include "common/types/time.hpp"

#include "common/string_util.hpp"
#include "common/exception.hpp"

#include <iomanip>
#include <iostream>
#include <sstream>

using namespace duckdb;
using namespace std;

// string format is hh:mm:ssZ
// Z is optional
// ISO 8601

// Taken from MonetDB mtime.c
#define TIME(h, m, s, x)                                                                                               \
	((h) >= 0 && (h) < 24 && (m) >= 0 && (m) < 60 && (s) >= 0 && (s) <= 60 && (x) >= 0 && (x) < 1000)

static dtime_t time_to_number(int hour, int min, int sec, int msec) {
	if (!TIME(hour, min, sec, msec)) {
		throw Exception("Invalid time");
	}
	return (dtime_t)(((((hour * 60) + min) * 60) + sec) * 1000 + msec);
}

static void number_to_time(dtime_t n, int32_t &hour, int32_t &min, int32_t &sec, int32_t &msec) {
	int h, m, s, ms;

	h = n / 3600000;
	n -= h * 3600000;
	m = n / 60000;
	n -= m * 60000;
	s = n / 1000;
	n -= s * 1000;
	ms = n;

	hour = h;
	min = m;
	sec = s;
	msec = ms;
}

dtime_t Time::FromString(string str) {
	int32_t hour, minute, second;
	char sep, sep2;
	istringstream ss(str);
	ss >> hour;
	ss >> sep;
	ss >> minute;
	ss >> sep2;
	ss >> second;

	if (ss.fail() || !IsValidTime(hour, minute, second) || sep != sep2 || sep != ':') {
		throw ConversionException("time field value out of range: \"%s\", "
		                          "expected format is (hh-mm-ss)",
		                          str.c_str());
	}
	return FromTime(hour, minute, second);
}

string Time::ToString(dtime_t time) {
	int32_t hour, min, sec, msec;
	number_to_time(time, hour, min, sec, msec);
	assert(msec == 0); // TODO add support for milliseconds
	return StringUtil::Format("%02d:%02d:%02d", hour, min, sec);
}

string Time::Format(int32_t hour, int32_t minute, int32_t second) {
	return ToString(Time::FromTime(hour, minute, second));
}

dtime_t Time::FromTime(int32_t hour, int32_t minute, int32_t second, int32_t milisecond) {
	return time_to_number(hour, minute, second, milisecond);
}

bool Time::IsValidTime(int32_t hour, int32_t minute, int32_t second) {
	return TIME(hour, minute, second, 0);
}

void Time::Convert(dtime_t time, int32_t &out_hour, int32_t &out_min, int32_t &out_sec, int32_t &out_msec) {
	number_to_time(time, out_hour, out_min, out_sec, out_msec);
}
