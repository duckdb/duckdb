#include "duckdb/common/types/timestamp.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/limits.hpp"
#include <ctime>

namespace duckdb {

static_assert(sizeof(timestamp_t) == sizeof(int64_t), "timestamp_t was padded");

// timestamp/datetime uses 64 bits, high 32 bits for date and low 32 bits for time
// string format is YYYY-MM-DDThh:mm:ssZ
// T may be a space
// Z is optional
// ISO 8601

// arithmetic operators
timestamp_t timestamp_t::operator+(const double &value) const {
	timestamp_t result;
	if (!TryAddOperator::Operation(this->value, int64_t(value), result.value)) {
		throw OutOfRangeException("Overflow in timestamp addition");
	}
	return result;
}

int64_t timestamp_t::operator-(const timestamp_t &other) const {
	int64_t result;
	if (!TrySubtractOperator::Operation(value, int64_t(other.value), result)) {
		throw OutOfRangeException("Overflow in timestamp subtraction");
	}
	return result;
}

// in-place operators
timestamp_t &timestamp_t::operator+=(const int64_t &delta) {
	if (!TryAddOperator::Operation(value, delta, value)) {
		throw OutOfRangeException("Overflow in timestamp increment");
	}
	return *this;
}

timestamp_t &timestamp_t::operator-=(const int64_t &delta) {
	if (!TrySubtractOperator::Operation(value, delta, value)) {
		throw OutOfRangeException("Overflow in timestamp decrement");
	}
	return *this;
}

bool Timestamp::TryConvertTimestampTZ(const char *str, idx_t len, timestamp_t &result, bool &has_offset, string_t &tz) {
	idx_t pos;
	date_t date;
	dtime_t time;
	has_offset = false;
	if (!Date::TryConvertDate(str, len, pos, date, has_offset)) {
		return false;
	}
	if (pos == len) {
		// no time: only a date or special
		if (date == date_t::infinity()) {
			result = timestamp_t::infinity();
			return true;
		} else if (date == date_t::ninfinity()) {
			result = timestamp_t::ninfinity();
			return true;
		}
		return Timestamp::TryFromDatetime(date, dtime_t(0), result);
	}
	// try to parse a time field
	if (str[pos] == ' ' || str[pos] == 'T') {
		pos++;
	}
	idx_t time_pos = 0;
	if (!Time::TryConvertTime(str + pos, len - pos, time_pos, time)) {
		return false;
	}
	pos += time_pos;
	if (!Timestamp::TryFromDatetime(date, time, result)) {
		return false;
	}
	if (pos < len) {
		// skip a "Z" at the end (as per the ISO8601 specs)
		int hour_offset, minute_offset;
		if (str[pos] == 'Z') {
			pos++;
			has_offset = true;
		} else if (Timestamp::TryParseUTCOffset(str, pos, len, hour_offset, minute_offset)) {
			const int64_t delta = hour_offset * Interval::MICROS_PER_HOUR + minute_offset * Interval::MICROS_PER_MINUTE;
			if (!TrySubtractOperator::Operation(result.value, delta, result.value)) {
				return false;
			}
			has_offset = true;
		} else {
			// Parse a time zone: / [A-Za-z0-9/_]+/
			if (str[pos++] != ' ') {
				return false;
			}
			auto tz_name = str + pos;
			for (; pos < len && CharacterIsTimeZone(str[pos]); ++pos) {
				continue;
			}
			auto tz_len = str + pos - tz_name;
			if (tz_len) {
				tz = string_t(tz_name, tz_len);
			}
			// Note that the caller must reinterpret the instant we return to the given time zone
		}

		// skip any spaces at the end
		while (pos < len && StringUtil::CharacterIsSpace(str[pos])) {
			pos++;
		}
		if (pos < len) {
			return false;
		}
	}
	return true;
}

TimestampCastResult Timestamp::TryConvertTimestamp(const char *str, idx_t len, timestamp_t &result) {
	string_t tz(nullptr, 0);
	bool has_offset = false;
	// We don't understand TZ without an extension, so fail if one was provided.
	auto success = TryConvertTimestampTZ(str, len, result, has_offset, tz);
	if (!success) {
		return TimestampCastResult::ERROR_INCORRECT_FORMAT;
	}
	if (tz.GetSize() == 0) {
		// no timezone provided - success!
		return TimestampCastResult::SUCCESS;
	}
	if (tz.GetSize() == 3) {
		// we can ONLY handle UTC without ICU being loaded
		auto tz_ptr = tz.GetData();
		if ((tz_ptr[0] == 'u' || tz_ptr[0] == 'U') && (tz_ptr[1] == 't' || tz_ptr[1] == 'T') &&
		    (tz_ptr[2] == 'c' || tz_ptr[2] == 'C')) {
			return TimestampCastResult::SUCCESS;
		}
	}
	return TimestampCastResult::ERROR_NON_UTC_TIMEZONE;
}

string Timestamp::ConversionError(const string &str) {
	return StringUtil::Format("timestamp field value out of range: \"%s\", "
	                          "expected format is (YYYY-MM-DD HH:MM:SS[.US][±HH:MM| ZONE])",
	                          str);
}

string Timestamp::UnsupportedTimezoneError(const string &str) {
	return StringUtil::Format("timestamp field value \"%s\" has a timestamp that is not UTC.\nUse the TIMESTAMPTZ type "
	                          "with the ICU extension loaded to handle non-UTC timestamps.",
	                          str);
}

string Timestamp::ConversionError(string_t str) {
	return Timestamp::ConversionError(str.GetString());
}

string Timestamp::UnsupportedTimezoneError(string_t str) {
	return Timestamp::UnsupportedTimezoneError(str.GetString());
}

timestamp_t Timestamp::FromCString(const char *str, idx_t len) {
	timestamp_t result;
	auto cast_result = Timestamp::TryConvertTimestamp(str, len, result);
	if (cast_result == TimestampCastResult::SUCCESS) {
		return result;
	}
	if (cast_result == TimestampCastResult::ERROR_NON_UTC_TIMEZONE) {
		throw ConversionException(Timestamp::UnsupportedTimezoneError(string(str, len)));
	} else {
		throw ConversionException(Timestamp::ConversionError(string(str, len)));
	}
}

bool Timestamp::TryParseUTCOffset(const char *str, idx_t &pos, idx_t len, int &hour_offset, int &minute_offset) {
	minute_offset = 0;
	idx_t curpos = pos;
	// parse the next 3 characters
	if (curpos + 3 > len) {
		// no characters left to parse
		return false;
	}
	char sign_char = str[curpos];
	if (sign_char != '+' && sign_char != '-') {
		// expected either + or -
		return false;
	}
	curpos++;
	if (!StringUtil::CharacterIsDigit(str[curpos]) || !StringUtil::CharacterIsDigit(str[curpos + 1])) {
		// expected +HH or -HH
		return false;
	}
	hour_offset = (str[curpos] - '0') * 10 + (str[curpos + 1] - '0');
	if (sign_char == '-') {
		hour_offset = -hour_offset;
	}
	curpos += 2;

	// optional minute specifier: expected either "MM" or ":MM"
	if (curpos >= len) {
		// done, nothing left
		pos = curpos;
		return true;
	}
	if (str[curpos] == ':') {
		curpos++;
	}
	if (curpos + 2 > len || !StringUtil::CharacterIsDigit(str[curpos]) ||
	    !StringUtil::CharacterIsDigit(str[curpos + 1])) {
		// no MM specifier
		pos = curpos;
		return true;
	}
	// we have an MM specifier: parse it
	minute_offset = (str[curpos] - '0') * 10 + (str[curpos + 1] - '0');
	if (sign_char == '-') {
		minute_offset = -minute_offset;
	}
	pos = curpos + 2;
	return true;
}

timestamp_t Timestamp::FromString(const string &str) {
	return Timestamp::FromCString(str.c_str(), str.size());
}

string Timestamp::ToString(timestamp_t timestamp) {
	if (timestamp == timestamp_t::infinity()) {
		return Date::PINF;
	} else if (timestamp == timestamp_t::ninfinity()) {
		return Date::NINF;
	}
	date_t date;
	dtime_t time;
	Timestamp::Convert(timestamp, date, time);
	return Date::ToString(date) + " " + Time::ToString(time);
}

date_t Timestamp::GetDate(timestamp_t timestamp) {
	if (timestamp == timestamp_t::infinity()) {
		return date_t::infinity();
	} else if (timestamp == timestamp_t::ninfinity()) {
		return date_t::ninfinity();
	}
	return date_t((timestamp.value + (timestamp.value < 0)) / Interval::MICROS_PER_DAY - (timestamp.value < 0));
}

dtime_t Timestamp::GetTime(timestamp_t timestamp) {
	if (!IsFinite(timestamp)) {
		throw ConversionException("Can't get TIME of infinite TIMESTAMP");
	}
	date_t date = Timestamp::GetDate(timestamp);
	return dtime_t(timestamp.value - (int64_t(date.days) * int64_t(Interval::MICROS_PER_DAY)));
}

bool Timestamp::TryFromDatetime(date_t date, dtime_t time, timestamp_t &result) {
	if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(date.days, Interval::MICROS_PER_DAY, result.value)) {
		return false;
	}
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(result.value, time.micros, result.value)) {
		return false;
	}
	return Timestamp::IsFinite(result);
}

timestamp_t Timestamp::FromDatetime(date_t date, dtime_t time) {
	timestamp_t result;
	if (!TryFromDatetime(date, time, result)) {
		throw Exception("Overflow exception in date/time -> timestamp conversion");
	}
	return result;
}

void Timestamp::Convert(timestamp_t timestamp, date_t &out_date, dtime_t &out_time) {
	out_date = GetDate(timestamp);
	int64_t days_micros;
	if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(out_date.days, Interval::MICROS_PER_DAY,
	                                                               days_micros)) {
		throw ConversionException("Date out of range in timestamp conversion");
	}
	out_time = dtime_t(timestamp.value - days_micros);
	D_ASSERT(timestamp == Timestamp::FromDatetime(out_date, out_time));
}

timestamp_t Timestamp::GetCurrentTimestamp() {
	auto now = system_clock::now();
	auto epoch_ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
	return Timestamp::FromEpochMs(epoch_ms);
}

timestamp_t Timestamp::FromEpochSeconds(int64_t sec) {
	int64_t result;
	if (!TryMultiplyOperator::Operation(sec, Interval::MICROS_PER_SEC, result)) {
		throw ConversionException("Could not convert Timestamp(S) to Timestamp(US)");
	}
	return timestamp_t(result);
}

timestamp_t Timestamp::FromEpochMs(int64_t ms) {
	int64_t result;
	if (!TryMultiplyOperator::Operation(ms, Interval::MICROS_PER_MSEC, result)) {
		throw ConversionException("Could not convert Timestamp(MS) to Timestamp(US)");
	}
	return timestamp_t(result);
}

timestamp_t Timestamp::FromEpochMicroSeconds(int64_t micros) {
	return timestamp_t(micros);
}

timestamp_t Timestamp::FromEpochNanoSeconds(int64_t ns) {
	return timestamp_t(ns / 1000);
}

int64_t Timestamp::GetEpochSeconds(timestamp_t timestamp) {
	return timestamp.value / Interval::MICROS_PER_SEC;
}

int64_t Timestamp::GetEpochMs(timestamp_t timestamp) {
	return timestamp.value / Interval::MICROS_PER_MSEC;
}

int64_t Timestamp::GetEpochMicroSeconds(timestamp_t timestamp) {
	return timestamp.value;
}

int64_t Timestamp::GetEpochNanoSeconds(timestamp_t timestamp) {
	int64_t result;
	int64_t ns_in_us = 1000;
	if (!TryMultiplyOperator::Operation(timestamp.value, ns_in_us, result)) {
		throw ConversionException("Could not convert Timestamp(US) to Timestamp(NS)");
	}
	return result;
}

double Timestamp::GetJulianDay(timestamp_t timestamp) {
	double result = Timestamp::GetTime(timestamp).micros;
	result /= Interval::MICROS_PER_DAY;
	result += Date::ExtractJulianDay(Timestamp::GetDate(timestamp));
	return result;
}

} // namespace duckdb
