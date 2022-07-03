#include "duckdb_python/python_objects.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

PyDictionary::PyDictionary(py::object dict) {
	keys = py::list(dict.attr("keys")());
	values = py::list(dict.attr("values")());
	len = py::len(keys);
	this->dict = move(dict);
}

PyTimeDelta::PyTimeDelta(py::handle &obj) {
	auto ptr = obj.ptr();
	days = PyDateTime_TIMEDELTA_GET_DAYS(ptr);
	seconds = PyDateTime_TIMEDELTA_GET_SECONDS(ptr);
	microseconds = PyDateTime_TIMEDELTA_GET_MICROSECONDS(ptr);
}

interval_t PyTimeDelta::ToInterval() {
	interval_t interval;

	//! Timedelta stores any amount of seconds lower than a day only
	D_ASSERT(seconds < Interval::SECS_PER_DAY);

	//! Convert overflow of days to months
	interval.months = days / Interval::DAYS_PER_MONTH;
	days -= interval.months * Interval::DAYS_PER_MONTH;

	microseconds += seconds * Interval::MICROS_PER_SEC;
	interval.days = days;
	interval.micros = microseconds;
	return interval;
}

PyTime::PyTime(py::handle &obj) : obj(obj) {
	auto ptr = obj.ptr();
	hour = PyDateTime_TIME_GET_HOUR(ptr);
	minute = PyDateTime_TIME_GET_MINUTE(ptr);
	second = PyDateTime_TIME_GET_SECOND(ptr);
	microsecond = PyDateTime_TIME_GET_MICROSECOND(ptr);
	timezone = PyDateTime_TIME_GET_TZINFO(ptr);
}
dtime_t PyTime::ToTime() {
	return Time::FromTime(hour, minute, second, microsecond);
}
Value PyTime::ToValue() {
	auto time = ToTime();
	if (timezone != Py_None) {
		auto utc_offset = PyTimezone::GetUTCOffset(timezone, *this);
		//! 'Add' requires a date_t for overflows
		date_t ignore;
		utc_offset = Interval::Invert(utc_offset);
		time = Interval::Add(time, utc_offset, ignore);
	}
	return Value::TIME(time);
}

interval_t PyTimezone::GetUTCOffset(PyObject *timezone, PyTime &time) {
	py::object tzinfo(timezone, true);
	auto res = tzinfo.attr("utcoffset")(time.obj);
	auto timedelta = PyTimeDelta(res);
	return timedelta.ToInterval();
}

interval_t PyTimezone::GetUTCOffset(PyObject *timezone, PyDateTime &datetime) {
	py::object tzinfo(timezone, true);
	auto res = tzinfo.attr("utcoffset")(datetime.obj);
	auto timedelta = PyTimeDelta(res);
	return timedelta.ToInterval();
}

PyDateTime::PyDateTime(py::handle &obj) : obj(obj) {
	auto ptr = obj.ptr();
	year = PyDateTime_GET_YEAR(ptr);
	month = PyDateTime_GET_MONTH(ptr);
	day = PyDateTime_GET_DAY(ptr);
	hour = PyDateTime_DATE_GET_HOUR(ptr);
	minute = PyDateTime_DATE_GET_MINUTE(ptr);
	second = PyDateTime_DATE_GET_SECOND(ptr);
	micros = PyDateTime_DATE_GET_MICROSECOND(ptr);
	timezone = PyDateTime_DATE_GET_TZINFO(ptr);
}

timestamp_t PyDateTime::ToTimestamp() {
	auto date = ToDate();
	auto time = ToTime();
	return Timestamp::FromDatetime(date, time);
}

Value PyDateTime::ToValue() {
	auto timestamp = ToTimestamp();
	if (timezone != Py_None) {
		auto utc_offset = PyTimezone::GetUTCOffset(timezone, *this);
		//! Need to subtract the UTC offset, so we invert the interval
		utc_offset = Interval::Invert(utc_offset);
		timestamp = Interval::Add(timestamp, utc_offset);
	}
	return Value::TIMESTAMP(timestamp);
}

date_t PyDateTime::ToDate() {
	return Date::FromDate(year, month, day);
}
dtime_t PyDateTime::ToTime() {
	return Time::FromTime(hour, minute, second, micros);
}

PyDate::PyDate(py::handle &ele) {
	auto ptr = ele.ptr();
	year = PyDateTime_GET_YEAR(ptr);
	month = PyDateTime_GET_MONTH(ptr);
	day = PyDateTime_GET_DAY(ptr);
}

Value PyDate::ToValue() {
	return Value::DATE(year, month, day);
}

} // namespace duckdb
