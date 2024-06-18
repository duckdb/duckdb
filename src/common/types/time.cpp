#include "duckdb/common/types/time.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

#include <cctype>
#include <cstring>
#include <sstream>

namespace duckdb {

static_assert(sizeof(dtime_t) == sizeof(int64_t), "dtime_t was padded");

// string format is hh:mm:ss.microsecondsZ
// microseconds and Z are optional
// ISO 8601

bool Time::TryConvertInternal(const char *buf, idx_t len, idx_t &pos, dtime_t &result, bool strict,
                              optional_ptr<int32_t> nanos) {
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

	// Allow up to 9 digit hours to support intervals
	hour = 0;
	for (int32_t digits = 9; pos < len && StringUtil::CharacterIsDigit(buf[pos]); ++pos) {
		if (digits-- > 0) {
			hour = hour * 10 + (buf[pos] - '0');
		} else {
			return false;
		}
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
		if (nanos) {
			// do we expect nanoseconds?
			mult *= Interval::NANOS_PER_MICRO;
		}
		for (; pos < len && StringUtil::CharacterIsDigit(buf[pos]); pos++, mult /= 10) {
			if (mult > 0) {
				micros += (buf[pos] - '0') * mult;
			}
		}
		if (nanos) {
			*nanos = micros % Interval::NANOS_PER_MICRO;
			micros /= Interval::NANOS_PER_MICRO;
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

bool Time::TryConvertInterval(const char *buf, idx_t len, idx_t &pos, dtime_t &result, bool strict,
                              optional_ptr<int32_t> nanos) {
	return Time::TryConvertInternal(buf, len, pos, result, strict, nanos);
}

bool Time::TryConvertTime(const char *buf, idx_t len, idx_t &pos, dtime_t &result, bool strict,
                          optional_ptr<int32_t> nanos) {
	if (!Time::TryConvertInternal(buf, len, pos, result, strict, nanos)) {
		if (!strict) {
			// last chance, check if we can parse as timestamp
			timestamp_t timestamp;
			if (Timestamp::TryConvertTimestamp(buf, len, timestamp, nanos) == TimestampCastResult::SUCCESS) {
				if (!Timestamp::IsFinite(timestamp)) {
					return false;
				}
				result = Timestamp::GetTime(timestamp);
				return true;
			}
		}
		return false;
	}
	return result.micros <= Interval::MICROS_PER_DAY;
}

bool Time::TryConvertTimeTZ(const char *buf, idx_t len, idx_t &pos, dtime_tz_t &result, bool &has_offset, bool strict,
                            optional_ptr<int32_t> nanos) {
	dtime_t time_part;
	has_offset = false;
	if (!Time::TryConvertInternal(buf, len, pos, time_part, false, nanos)) {
		if (!strict) {
			// last chance, check if we can parse as timestamp
			timestamp_t timestamp;
			if (Timestamp::TryConvertTimestamp(buf, len, timestamp, nanos) == TimestampCastResult::SUCCESS) {
				if (!Timestamp::IsFinite(timestamp)) {
					return false;
				}
				result = dtime_tz_t(Timestamp::GetTime(timestamp), 0);
				return true;
			}
		}
		return false;
	}

	// skip optional whitespace before offset
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}

	//	Get the ±HH[:MM] part
	int hh = 0;
	int mm = 0;
	has_offset = (pos < len);
	if (has_offset && !Timestamp::TryParseUTCOffset(buf, pos, len, hh, mm)) {
		return false;
	}

	//	Offsets are in seconds in the open interval (-16:00:00, +16:00:00)
	int32_t offset = ((hh * Interval::MINS_PER_HOUR) + mm) * Interval::SECS_PER_MINUTE;

	//	Check for trailing seconds.
	//	(PG claims they don't support this but they do...)
	if (pos < len && buf[pos] == ':') {
		++pos;
		int ss = 0;
		if (!Date::ParseDoubleDigit(buf, len, pos, ss)) {
			return false;
		}
		offset += (offset < 0) ? -ss : ss;
	}

	if (offset < dtime_tz_t::MIN_OFFSET || offset > dtime_tz_t::MAX_OFFSET) {
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

dtime_t Time::NormalizeTimeTZ(dtime_tz_t timetz) {
	date_t date(0);
	return Interval::Add(timetz.time(), {0, 0, -timetz.offset() * Interval::MICROS_PER_SEC}, date);
}

string Time::ConversionError(const string &str) {
	return StringUtil::Format("time field value out of range: \"%s\", "
	                          "expected format is ([YYYY-MM-DD ]HH:MM:SS[.MS])",
	                          str);
}

string Time::ConversionError(string_t str) {
	return Time::ConversionError(str.GetString());
}

dtime_t Time::FromCString(const char *buf, idx_t len, bool strict, optional_ptr<int32_t> nanos) {
	dtime_t result;
	idx_t pos;
	if (!Time::TryConvertTime(buf, len, pos, result, strict, nanos)) {
		throw ConversionException(ConversionError(string(buf, len)));
	}
	return result;
}

dtime_t Time::FromString(const string &str, bool strict, optional_ptr<int32_t> nanos) {
	return Time::FromCString(str.c_str(), str.size(), strict, nanos);
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

int64_t Time::ToNanoTime(int32_t hour, int32_t minute, int32_t second, int32_t nanoseconds) {
	int64_t result;
	result = hour;                                           // hours
	result = result * Interval::MINS_PER_HOUR + minute;      // hours -> minutes
	result = result * Interval::SECS_PER_MINUTE + second;    // minutes -> seconds
	result = result * Interval::NANOS_PER_SEC + nanoseconds; // seconds -> nanoseconds
	return result;
}

bool Time::IsValidTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
	if (hour < 0 || hour >= 24) {
		return (hour == 24) && (minute == 0) && (second == 0) && (microseconds == 0);
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
