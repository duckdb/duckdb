#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"

#include <iomanip>
#include <cstring>
#include <iostream>
#include <sstream>
#include <cctype>

using namespace duckdb;
using namespace std;

// string format is hh:mm:ssZ
// Z is optional
// ISO 8601

// Taken from MonetDB mtime.c
#define DD_TIME(h, m, s, x)                                                                                            \
	((h) >= 0 && (h) < 24 && (m) >= 0 && (m) < 60 && (s) >= 0 && (s) <= 60 && (x) >= 0 && (x) < 1000)

static dtime_t time_to_number(int hour, int min, int sec, int msec) {
	if (!DD_TIME(hour, min, sec, msec)) {
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

// TODO this is duplicated in date.cpp
static bool ParseDoubleDigit2(const char *buf, idx_t &pos, int32_t &result) {
	if (std::isdigit((unsigned char)buf[pos])) {
		result = buf[pos++] - '0';
		if (std::isdigit((unsigned char)buf[pos])) {
			result = (buf[pos++] - '0') + result * 10;
		}
		return true;
	}
	return false;
}

static bool TryConvertTime(const char *buf, dtime_t &result) {
	int32_t hour = -1, min = -1, sec = -1, msec = -1;
	idx_t pos = 0;
	int sep;

	// skip leading spaces
	while (std::isspace((unsigned char)buf[pos])) {
		pos++;
	}

	if (!std::isdigit((unsigned char)buf[pos])) {
		return false;
	}

	if (!ParseDoubleDigit2(buf, pos, hour)) {
		return false;
	}
	if (hour < 0 || hour > 24) {
		return false;
	}

	// fetch the separator
	sep = buf[pos++];
	if (sep != ':') {
		// invalid separator
		return false;
	}

	if (!ParseDoubleDigit2(buf, pos, min)) {
		return false;
	}
	if (min < 0 || min > 60) {
		return false;
	}

	if (buf[pos++] != sep) {
		return false;
	}

	if (!ParseDoubleDigit2(buf, pos, sec)) {
		return false;
	}
	if (sec < 0 || sec > 60) {
		return false;
	}

	msec = 0;
	sep = buf[pos++];
	if (sep == '.') { // we expect some milliseconds
		uint8_t mult = 100;
		for (; std::isdigit((unsigned char)buf[pos]) && mult > 0; pos++, mult /= 10) {
			msec += (buf[pos] - '0') * mult;
		}
	}

	result = Time::FromTime(hour, min, sec, msec);
	return true;
}

dtime_t Time::FromCString(const char *buf) {
	dtime_t result;
	if (!TryConvertTime(buf, result)) {
		// last chance, check if we can parse as timestamp
		if (strlen(buf) > 10) {
			return Timestamp::GetTime(Timestamp::FromString(buf));
		}
		throw ConversionException("time field value out of range: \"%s\", "
		                          "expected format is ([YYY-MM-DD ]HH:MM:SS[.MS])",
		                          buf);
	}
	return result;
}

dtime_t Time::FromString(string str) {
	return Time::FromCString(str.c_str());
}

string Time::ToString(dtime_t time) {
	int32_t hour, min, sec, msec;
	number_to_time(time, hour, min, sec, msec);

	if (msec > 0) {
		return StringUtil::Format("%02d:%02d:%02d.%03d", hour, min, sec, msec);
	} else {
		return StringUtil::Format("%02d:%02d:%02d", hour, min, sec);
	}
}

string Time::Format(int32_t hour, int32_t minute, int32_t second, int32_t milisecond) {
	return ToString(Time::FromTime(hour, minute, second, milisecond));
}

dtime_t Time::FromTime(int32_t hour, int32_t minute, int32_t second, int32_t milisecond) {
	return time_to_number(hour, minute, second, milisecond);
}

bool Time::IsValidTime(int32_t hour, int32_t minute, int32_t second, int32_t milisecond) {
	return DD_TIME(hour, minute, second, milisecond);
}

void Time::Convert(dtime_t time, int32_t &out_hour, int32_t &out_min, int32_t &out_sec, int32_t &out_msec) {
	number_to_time(time, out_hour, out_min, out_sec, out_msec);
}
