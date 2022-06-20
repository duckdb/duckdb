#include "duckdb/common/types/timestamp.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/limits.hpp"
#include <ctime>

namespace duckdb {

static_assert(sizeof(timestamp_t) == sizeof(int64_t), "timestamp_t was padded");

// timestamp/datetime uses 64 bits, high 32 bits for date and low 32 bits for time
// string format is YYYY-MM-DDThh:mm:ssZ
// T may be a space
// Z is optional
// ISO 8601
bool Timestamp::TryConvertTimestamp(const char *str, idx_t len, timestamp_t &result) {
	idx_t pos;
	date_t date;
	dtime_t time;
	if (!Date::TryConvertDate(str, len, pos, date)) {
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
		if (str[pos] == 'Z') {
			pos++;
		}
		int hour_offset, minute_offset;
		if (Timestamp::TryParseUTCOffset(str, pos, len, hour_offset, minute_offset)) {
			result -= hour_offset * Interval::MICROS_PER_HOUR + minute_offset * Interval::MICROS_PER_MINUTE;
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

string Timestamp::ConversionError(const string &str) {
	return StringUtil::Format("timestamp field value out of range: \"%s\", "
	                          "expected format is (YYYY-MM-DD HH:MM:SS[.MS])",
	                          str);
}

string Timestamp::ConversionError(string_t str) {
	return Timestamp::ConversionError(str.GetString());
}

timestamp_t Timestamp::FromCString(const char *str, idx_t len) {
	timestamp_t result;
	if (!Timestamp::TryConvertTimestamp(str, len, result)) {
		throw ConversionException(Timestamp::ConversionError(string(str, len)));
	}
	return result;
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
		return "infinity";
	} else if (timestamp == timestamp_t::ninfinity()) {
		return "-infinity";
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

} // namespace duckdb
