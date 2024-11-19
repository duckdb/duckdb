#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

using duckdb::Date;
using duckdb::Time;
using duckdb::Timestamp;

using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::timestamp_ms_t;
using duckdb::timestamp_ns_t;
using duckdb::timestamp_sec_t;
using duckdb::timestamp_t;

duckdb_date_struct duckdb_from_date(duckdb_date date) {
	int32_t year, month, day;
	Date::Convert(date_t(date.days), year, month, day);

	duckdb_date_struct result;
	result.year = year;
	result.month = duckdb::UnsafeNumericCast<int8_t>(month);
	result.day = duckdb::UnsafeNumericCast<int8_t>(day);
	return result;
}

duckdb_date duckdb_to_date(duckdb_date_struct date) {
	duckdb_date result;
	result.days = Date::FromDate(date.year, date.month, date.day).days;
	return result;
}

bool duckdb_is_finite_date(duckdb_date date) {
	return Date::IsFinite(date_t(date.days));
}

duckdb_time_struct duckdb_from_time(duckdb_time time) {
	int32_t hour, minute, second, micros;
	Time::Convert(dtime_t(time.micros), hour, minute, second, micros);

	duckdb_time_struct result;
	result.hour = duckdb::UnsafeNumericCast<int8_t>(hour);
	result.min = duckdb::UnsafeNumericCast<int8_t>(minute);
	result.sec = duckdb::UnsafeNumericCast<int8_t>(second);
	result.micros = micros;
	return result;
}

duckdb_time_tz_struct duckdb_from_time_tz(duckdb_time_tz input) {
	duckdb_time_tz_struct result;
	duckdb_time time;

	duckdb::dtime_tz_t time_tz(input.bits);

	time.micros = time_tz.time().micros;

	result.time = duckdb_from_time(time);
	result.offset = time_tz.offset();
	return result;
}

duckdb_time_tz duckdb_create_time_tz(int64_t micros, int32_t offset) {
	duckdb_time_tz time;
	time.bits = duckdb::dtime_tz_t(duckdb::dtime_t(micros), offset).bits;
	return time;
}

duckdb_time duckdb_to_time(duckdb_time_struct time) {
	duckdb_time result;
	result.micros = Time::FromTime(time.hour, time.min, time.sec, time.micros).micros;
	return result;
}

duckdb_timestamp_struct duckdb_from_timestamp(duckdb_timestamp ts) {
	date_t date;
	dtime_t time;
	Timestamp::Convert(timestamp_t(ts.micros), date, time);

	duckdb_date ddate;
	ddate.days = date.days;

	duckdb_time dtime;
	dtime.micros = time.micros;

	duckdb_timestamp_struct result;
	result.date = duckdb_from_date(ddate);
	result.time = duckdb_from_time(dtime);
	return result;
}

duckdb_timestamp duckdb_to_timestamp(duckdb_timestamp_struct ts) {
	auto date = date_t(duckdb_to_date(ts.date).days);
	auto time = dtime_t(duckdb_to_time(ts.time).micros);

	duckdb_timestamp result;
	result.micros = Timestamp::FromDatetime(date, time).value;
	return result;
}

bool duckdb_is_finite_timestamp(duckdb_timestamp ts) {
	return Timestamp::IsFinite(timestamp_t(ts.micros));
}

duckdb_timestamp_struct duckdb_from_timestamp_s(duckdb_timestamp_s ts_s) {
	date_t date;
	dtime_t time;
	auto ts_us = Timestamp::FromEpochSecondsPossiblyInfinite(ts_s.seconds);
	Timestamp::Convert(ts_us, date, time);

	duckdb_date ddate;
	ddate.days = date.days;

	duckdb_time dtime;
	dtime.micros = time.micros;

	duckdb_timestamp_struct result;
	result.date = duckdb_from_date(ddate);
	result.time = duckdb_from_time(dtime);
	return result;
}

duckdb_timestamp_s duckdb_to_timestamp_s(duckdb_timestamp_struct ts) {
	auto date = date_t(duckdb_to_date(ts.date).days);
	auto time = dtime_t(duckdb_to_time(ts.time).micros);

	duckdb_timestamp_s result;
	result.seconds = Timestamp::GetEpochSeconds(Timestamp::FromDatetime(date, time));
	return result;
}

bool duckdb_is_finite_timestamp_s(duckdb_timestamp_s ts_s) {
	return Timestamp::IsFinite(timestamp_t(ts_s.seconds));
}

duckdb_timestamp_struct duckdb_from_timestamp_ms(duckdb_timestamp_ms ts_ms) {
	date_t date;
	dtime_t time;
	auto ts_us = Timestamp::FromEpochMsPossiblyInfinite(ts_ms.millis);
	Timestamp::Convert(ts_us, date, time);

	duckdb_date ddate;
	ddate.days = date.days;

	duckdb_time dtime;
	dtime.micros = time.micros;

	duckdb_timestamp_struct result;
	result.date = duckdb_from_date(ddate);
	result.time = duckdb_from_time(dtime);
	return result;
}

duckdb_timestamp_ms duckdb_to_timestamp_ms(duckdb_timestamp_struct ts) {
	auto date = date_t(duckdb_to_date(ts.date).days);
	auto time = dtime_t(duckdb_to_time(ts.time).micros);

	duckdb_timestamp_ms result;
	result.millis = Timestamp::GetEpochMs(Timestamp::FromDatetime(date, time));
	return result;
}

bool duckdb_is_finite_timestamp_ms(duckdb_timestamp_ms ts_ms) {
	return Timestamp::IsFinite(timestamp_t(ts_ms.millis));
}

duckdb_state duckdb_from_timestamp_ns(duckdb_timestamp_ns ts_ns, duckdb_timestamp_ns_struct *ts_ns_struct) {
	if (!ts_ns_struct) {
		return DuckDBError;
	}

	date_t date;
	dtime_t time;
	int32_t nanos;
	timestamp_ns_t ts_ns_t;
	ts_ns_t.value = ts_ns.nanos;
	try {
		Timestamp::Convert(ts_ns_t, date, time, nanos);
	} catch (...) {
		return DuckDBError;
	}

	duckdb_date ddate;
	ddate.days = date.days;

	duckdb_time dtime;
	dtime.micros = time.micros;

	ts_ns_struct->date = duckdb_from_date(ddate);
	ts_ns_struct->time = duckdb_from_time(dtime);
	ts_ns_struct->nanos = nanos;
	return DuckDBSuccess;
}

duckdb_state duckdb_to_timestamp_ns(duckdb_timestamp_ns_struct ts_ns_struct, duckdb_timestamp_ns *ts_ns) {
	if (!ts_ns) {
		return DuckDBError;
	}

	date_t date;
	if (!Date::TryFromDate(ts_ns_struct.date.year, ts_ns_struct.date.month, ts_ns_struct.date.day, date)) {
		return DuckDBError;
	}

	auto time =
	    Time::FromTime(ts_ns_struct.time.hour, ts_ns_struct.time.min, ts_ns_struct.time.sec, ts_ns_struct.time.micros);
	auto nanos = ts_ns_struct.nanos;

	timestamp_t ts_result;
	if (!Timestamp::TryFromDatetime(date, time, ts_result)) {
		return DuckDBError;
	}

	timestamp_ns_t ts_ns_result;
	if (!Timestamp::TryFromTimestampNanos(ts_result, nanos, ts_ns_result)) {
		return DuckDBError;
	}

	ts_ns->nanos = ts_ns_result.value;
	return DuckDBSuccess;
}

bool duckdb_is_finite_timestamp_ns(duckdb_timestamp_ns ts_ns) {
	return Timestamp::IsFinite(timestamp_t(ts_ns.nanos));
}
