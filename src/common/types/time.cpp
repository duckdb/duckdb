#include "duckdb/common/types/time.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/operator/multiply.hpp"

#include <cctype>
#include <cstring>
#include <sstream>

namespace duckdb {

static_assert(sizeof(dtime_t) == sizeof(int64_t), "dtime_t was padded");

// string format is hh:mm:ss.microsecondsZ
// microseconds and Z are optional
// ISO 8601

bool Time::TryConvertInternal(const char *buf, idx_t len, idx_t &pos, dtime_t &result, bool strict) {
	int32_t hour = -1, min = -1, sec = -1, micros = -1;
	pos = 0;

	if (len == 0) {
		return false;
	}

	int sep;

	// skip leading spaces
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}

	if (pos >= len) {
		return false;
	}

	if (!StringUtil::CharacterIsDigit(buf[pos])) {
		return false;
	}

	if (!Date::ParseDoubleDigit(buf, len, pos, hour)) {
		return false;
	}
	if (hour < 0 || hour >= 24) {
		return false;
	}

	if (pos >= len) {
		return false;
	}

	// fetch the separator
	sep = buf[pos++];
	if (sep != ':') {
		// invalid separator
		return false;
	}

	if (!Date::ParseDoubleDigit(buf, len, pos, min)) {
		return false;
	}
	if (min < 0 || min >= 60) {
		return false;
	}

	if (pos >= len) {
		return false;
	}

	if (buf[pos++] != sep) {
		return false;
	}

	if (!Date::ParseDoubleDigit(buf, len, pos, sec)) {
		return false;
	}
	if (sec < 0 || sec >= 60) {
		return false;
	}

	micros = 0;
	if (pos < len && buf[pos] == '.') {
		pos++;
		// we expect some microseconds
		int32_t mult = 100000;
		for (; pos < len && StringUtil::CharacterIsDigit(buf[pos]); pos++, mult /= 10) {
			if (mult > 0) {
				micros += (buf[pos] - '0') * mult;
			}
		}
	}

	// in strict mode, check remaining string for non-space characters
	if (strict) {
		// skip trailing spaces
		while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
			pos++;
		}
		// check position. if end was not reached, non-space chars remaining
		if (pos < len) {
			return false;
		}
	}

	result = Time::FromTime(hour, min, sec, micros);
	return true;
}

bool Time::TryConvertTime(const char *buf, idx_t len, idx_t &pos, dtime_t &result, bool strict) {
	if (!Time::TryConvertInternal(buf, len, pos, result, strict)) {
		if (!strict) {
			// last chance, check if we can parse as timestamp
			timestamp_t timestamp;
			if (Timestamp::TryConvertTimestamp(buf, len, timestamp) == TimestampCastResult::SUCCESS) {
				if (!Timestamp::IsFinite(timestamp)) {
					return false;
				}
				result = Timestamp::GetTime(timestamp);
				return true;
			}
		}
		return false;
	}
	return true;
}

bool Time::TryParseUTCOffset(const char *str, idx_t &pos, idx_t len, int32_t &offset) {
	offset = 0;
	if (pos == len || StringUtil::CharacterIsSpace(str[pos])) {
		return true;
	}

	idx_t curpos = pos;
	// Minimum of 3 characters
	if (curpos + 3 > len) {
		// no characters left to parse
		return false;
	}

	const auto sign_char = str[curpos];
	if (sign_char != '+' && sign_char != '-') {
		// expected either + or -
		return false;
	}
	curpos++;

	int32_t hh = 0;
	idx_t start = curpos;
	for (; curpos < len; ++curpos) {
		const auto c = str[curpos];
		if (!StringUtil::CharacterIsDigit(c)) {
			break;
		}
		hh = hh * 10 + (c - '0');
	}
	//	HH is in [-1559,+1559] and must be at least two digits
	if (curpos - start < 2 || hh > 1559) {
		return false;
	}

	// optional minute specifier: expected ":MM"
	int32_t mm = 0;
	if (curpos + 3 <= len && str[curpos] == ':') {
		++curpos;
		if (!Date::ParseDoubleDigit(str, len, curpos, mm) || mm >= Interval::MINS_PER_HOUR) {
			return false;
		}
	}

	// optional seconds specifier: expected ":SS"
	int32_t ss = 0;
	if (curpos + 3 <= len && str[curpos] == ':') {
		++curpos;
		if (!Date::ParseDoubleDigit(str, len, curpos, ss) || ss >= Interval::SECS_PER_MINUTE) {
			return false;
		}
	}

	//	Assemble the offset now that we know nothing went wrong
	offset += hh * Interval::SECS_PER_HOUR;
	offset += mm * Interval::SECS_PER_MINUTE;
	offset += ss;
	if (sign_char == '-') {
		offset = -offset;
	}

	pos = curpos;

	return true;
}

bool Time::TryConvertTimeTZ(const char *buf, idx_t len, idx_t &pos, dtime_tz_t &result, bool strict) {
	dtime_t time_part;
	if (!Time::TryConvertInternal(buf, len, pos, time_part, false)) {
		if (!strict) {
			// last chance, check if we can parse as timestamp
			timestamp_t timestamp;
			if (Timestamp::TryConvertTimestamp(buf, len, timestamp) == TimestampCastResult::SUCCESS) {
				if (!Timestamp::IsFinite(timestamp)) {
					return false;
				}
				result = dtime_tz_t(Timestamp::GetTime(timestamp), 0);
				return true;
			}
		}
		return false;
	}

	//	We can't use Timestamp::TryParseUTCOffset because the colon is optional there but required here.
	int32_t offset = 0;
	if (!TryParseUTCOffset(buf, pos, len, offset)) {
		return false;
	}

	// in strict mode, check remaining string for non-space characters
	if (strict) {
		// skip trailing spaces
		while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
			pos++;
		}
		// check position. if end was not reached, non-space chars remaining
		if (pos < len) {
			return false;
		}
	}

	result = dtime_tz_t(time_part, offset);

	return true;
}

string Time::ConversionError(const string &str) {
	return StringUtil::Format("time field value out of range: \"%s\", "
	                          "expected format is ([YYYY-MM-DD ]HH:MM:SS[.MS])",
	                          str);
}

string Time::ConversionError(string_t str) {
	return Time::ConversionError(str.GetString());
}

dtime_t Time::FromCString(const char *buf, idx_t len, bool strict) {
	dtime_t result;
	idx_t pos;
	if (!Time::TryConvertTime(buf, len, pos, result, strict)) {
		throw ConversionException(ConversionError(string(buf, len)));
	}
	return result;
}

dtime_t Time::FromString(const string &str, bool strict) {
	return Time::FromCString(str.c_str(), str.size(), strict);
}

string Time::ToString(dtime_t time) {
	int32_t time_units[4];
	Time::Convert(time, time_units[0], time_units[1], time_units[2], time_units[3]);

	char micro_buffer[6];
	auto length = TimeToStringCast::Length(time_units, micro_buffer);
	auto buffer = make_unsafe_uniq_array<char>(length);
	TimeToStringCast::Format(buffer.get(), length, time_units, micro_buffer);
	return string(buffer.get(), length);
}

string Time::ToUTCOffset(int hour_offset, int minute_offset) {
	dtime_t time((hour_offset * Interval::MINS_PER_HOUR + minute_offset) * Interval::MICROS_PER_MINUTE);

	char buffer[1 + 2 + 1 + 2];
	idx_t length = 0;
	buffer[length++] = (time.micros < 0 ? '-' : '+');
	time.micros = std::abs(time.micros);

	int32_t time_units[4];
	Time::Convert(time, time_units[0], time_units[1], time_units[2], time_units[3]);

	TimeToStringCast::FormatTwoDigits(buffer + length, time_units[0]);
	length += 2;
	if (time_units[1]) {
		buffer[length++] = ':';
		TimeToStringCast::FormatTwoDigits(buffer + length, time_units[1]);
		length += 2;
	}

	return string(buffer, length);
}

dtime_t Time::FromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
	int64_t result;
	result = hour;                                             // hours
	result = result * Interval::MINS_PER_HOUR + minute;        // hours -> minutes
	result = result * Interval::SECS_PER_MINUTE + second;      // minutes -> seconds
	result = result * Interval::MICROS_PER_SEC + microseconds; // seconds -> microseconds
	return dtime_t(result);
}

bool Time::IsValidTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
	if (hour < 0 || hour >= 24) {
		return false;
	}
	if (minute < 0 || minute >= 60) {
		return false;
	}
	if (second < 0 || second > 60) {
		return false;
	}
	if (microseconds < 0 || microseconds > 1000000) {
		return false;
	}
	return true;
}

void Time::Convert(dtime_t dtime, int32_t &hour, int32_t &min, int32_t &sec, int32_t &micros) {
	int64_t time = dtime.micros;
	hour = int32_t(time / Interval::MICROS_PER_HOUR);
	time -= int64_t(hour) * Interval::MICROS_PER_HOUR;
	min = int32_t(time / Interval::MICROS_PER_MINUTE);
	time -= int64_t(min) * Interval::MICROS_PER_MINUTE;
	sec = int32_t(time / Interval::MICROS_PER_SEC);
	time -= int64_t(sec) * Interval::MICROS_PER_SEC;
	micros = int32_t(time);
	D_ASSERT(Time::IsValidTime(hour, min, sec, micros));
}

dtime_t Time::FromTimeMs(int64_t time_ms) {
	int64_t result;
	if (!TryMultiplyOperator::Operation(time_ms, Interval::MICROS_PER_MSEC, result)) {
		throw ConversionException("Could not convert Time(MS) to Time(US)");
	}
	return dtime_t(result);
}

dtime_t Time::FromTimeNs(int64_t time_ns) {
	return dtime_t(time_ns / Interval::NANOS_PER_MICRO);
}

} // namespace duckdb
