#include "duckdb/common/types/timestamp.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/string_util.hpp"

#include <chrono> // chrono::system_clock
#include <ctime>

using namespace std;

namespace duckdb {

// timestamp/datetime uses 64 bits, high 32 bits for date and low 32 bits for time
// string format is YYYY-MM-DDThh:mm:ssZ
// T may be a space
// Z is optional
// ISO 8601

timestamp_t Timestamp::FromCString(const char *str, idx_t len) {
	idx_t pos;
	date_t date;
	dtime_t time;
	if (!Date::TryConvertDate(str, len, pos, date)) {
		throw ConversionException("timestamp field value out of range: \"%s\", "
		                          "expected format is (YYYY-MM-DD HH:MM:SS[.MS])",
		                          string(str, len));
	}
	if (pos == len) {
		// no time: only a date
		return Timestamp::FromDatetime(date, 0);
	}
	// try to parse a time field
	if (str[pos] == ' ' || str[pos] == 'T') {
		pos++;
	}
	idx_t time_pos = 0;
	if (!Time::TryConvertTime(str + pos, len - pos, time_pos, time)) {
		throw ConversionException("timestamp field value out of range: \"%s\", "
		                          "expected format is (YYYY-MM-DD HH:MM:SS[.MS])",
		                          string(str, len));
	}
	pos += time_pos;
	if (pos < len) {
		// skip a "Z" at the end (as per the ISO8601 specs)
		if (str[pos] == 'Z') {
			pos++;
		}
		// skip any spaces at the end
		while (pos < len && StringUtil::CharacterIsSpace(str[pos])) {
			pos++;
		}
		if (pos < len) {
			throw ConversionException("timestamp field value out of range: \"%s\", "
			                          "expected format is (YYYY-MM-DD HH:MM:SS[.MS])",
			                          string(str, len));
		}
	}
	return Timestamp::FromDatetime(date, time);
}

timestamp_t Timestamp::FromString(string str) {
	return Timestamp::FromCString(str.c_str(), str.size());
}

string Timestamp::ToString(timestamp_t timestamp) {
	return Date::ToString(GetDate(timestamp)) + " " + Time::ToString(GetTime(timestamp));
}

date_t Timestamp::GetDate(timestamp_t timestamp) {
	return timestamp / Interval::MICROS_PER_DAY - (timestamp < 0);
}

dtime_t Timestamp::GetTime(timestamp_t timestamp) {
	return (timestamp - (Timestamp::GetDate(timestamp) * Interval::MICROS_PER_DAY));
}

timestamp_t Timestamp::FromDatetime(date_t date, dtime_t time) {
	return date * Interval::MICROS_PER_DAY + time;
}

void Timestamp::Convert(timestamp_t timestamp, date_t &out_date, dtime_t &out_time) {
	out_date = GetDate(timestamp);
	out_time = timestamp - (out_date * Interval::MICROS_PER_DAY);
}

timestamp_t Timestamp::GetCurrentTimestamp() {
	using std::chrono::system_clock;
	auto now = std::chrono::system_clock::now();
	auto epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
	return Timestamp::FromEpochMs(epoch_ms);
}

timestamp_t Timestamp::FromEpochSeconds(int64_t sec) {
	return sec * Interval::MICROS_PER_SEC;
}

timestamp_t Timestamp::FromEpochMs(int64_t ms) {
	return ms * Interval::MICROS_PER_MSEC;
}

timestamp_t Timestamp::FromEpochMicroSeconds(int64_t micros) {
	return micros;
}

int64_t Timestamp::GetEpochSeconds(timestamp_t timestamp) {
	return timestamp / Interval::MICROS_PER_SEC;
}

int64_t Timestamp::GetEpochMs(timestamp_t timestamp) {
	return timestamp / Interval::MICROS_PER_MSEC;
}

int64_t Timestamp::GetEpochMicroSeconds(timestamp_t timestamp) {
	return timestamp;
}

int64_t Timestamp::GetEpochNanoSeconds(timestamp_t timestamp) {
	return timestamp * 1000;
}

} // namespace duckdb
