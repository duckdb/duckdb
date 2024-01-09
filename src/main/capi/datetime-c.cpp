#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

using duckdb::Date;
using duckdb::Time;
using duckdb::Timestamp;

using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::timestamp_t;

duckdb_date_struct duckdb_from_date(duckdb_date date) {
	int32_t year, month, day;
	Date::Convert(date_t(date.days), year, month, day);

	duckdb_date_struct result;
	result.year = year;
	result.month = month;
	result.day = day;
	return result;
}

duckdb_date duckdb_to_date(duckdb_date_struct date) {
	duckdb_date result;
	result.days = Date::FromDate(date.year, date.month, date.day).days;
	return result;
}

duckdb_time_struct duckdb_from_time(duckdb_time time) {
	int32_t hour, minute, second, micros;
	Time::Convert(dtime_t(time.micros), hour, minute, second, micros);

	duckdb_time_struct result;
	result.hour = hour;
	result.min = minute;
	result.sec = second;
	result.micros = micros;
	return result;
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
	date_t date = date_t(duckdb_to_date(ts.date).days);
	dtime_t time = dtime_t(duckdb_to_time(ts.time).micros);

	duckdb_timestamp result;
	result.micros = Timestamp::FromDatetime(date, time).value;
	return result;
}
