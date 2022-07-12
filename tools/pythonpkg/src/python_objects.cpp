#include "duckdb_python/python_objects.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/decimal.hpp"

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

PyDecimal::PyDecimal(py::handle &obj) {
	auto as_tuple = obj.attr("as_tuple")();
	auto _digits = as_tuple.attr("digits");
	exponent = py::cast<int32_t>(as_tuple.attr("exponent"));
	auto width = py::len(_digits);
	auto sign = py::cast<int8_t>(as_tuple.attr("sign"));
	signed_value = sign != 0;

	exponent *= -1;
	digits.reserve(width);
	for (auto digit : _digits) {
		digits.push_back(py::cast<int8_t>(digit));
	}
}

LogicalType PyDecimal::GetType() {
	int32_t width = digits.size();
	auto scale = exponent;
	return LogicalType::DECIMAL(width, scale);
}

Value PyDecimal::ToDuckValue() {
	int32_t width = digits.size();
	auto scale = exponent;

	if (width > Decimal::MAX_WIDTH_INT64) {
		throw std::runtime_error("Decimal value exceeds max supported width, failed to convert");
	}
	int64_t value = 0;
	for (auto it = digits.begin(); it != digits.end(); it++) {
		value = value * 10 + *it;
	}
	if (signed_value) {
		value = -value;
	}
	return Value::DECIMAL(value, width, scale);
}

PyTime::PyTime(py::handle &obj) : obj(obj) {
	auto ptr = obj.ptr();
	hour = PyDateTime_TIME_GET_HOUR(ptr);
	minute = PyDateTime_TIME_GET_MINUTE(ptr);
	second = PyDateTime_TIME_GET_SECOND(ptr);
	microsecond = PyDateTime_TIME_GET_MICROSECOND(ptr);
	timezone_obj = PyDateTime_TIME_GET_TZINFO(ptr);
}
dtime_t PyTime::ToDuckTime() {
	return Time::FromTime(hour, minute, second, microsecond);
}
Value PyTime::ToDuckValue() {
	auto duckdb_time = this->ToDuckTime();
	if (this->timezone_obj != Py_None) {
		auto utc_offset = PyTimezone::GetUTCOffset(this->timezone_obj);
		//! 'Add' requires a date_t for overflows
		date_t ignored_date;
		utc_offset = Interval::Invert(utc_offset);
		duckdb_time = Interval::Add(duckdb_time, utc_offset, ignored_date);
	}
	return Value::TIME(duckdb_time);
}

interval_t PyTimezone::GetUTCOffset(PyObject *tzone_obj) {
	auto tzinfo = py::reinterpret_borrow<py::object>(tzone_obj);
	auto res = tzinfo.attr("utcoffset")(py::none());
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
	tzone_obj = PyDateTime_DATE_GET_TZINFO(ptr);
}

timestamp_t PyDateTime::ToTimestamp() {
	auto date = ToDate();
	auto time = ToDuckTime();
	return Timestamp::FromDatetime(date, time);
}

Value PyDateTime::ToDuckValue() {
	auto timestamp = ToTimestamp();
	if (tzone_obj != Py_None) {
		auto utc_offset = PyTimezone::GetUTCOffset(tzone_obj);
		//! Need to subtract the UTC offset, so we invert the interval
		utc_offset = Interval::Invert(utc_offset);
		timestamp = Interval::Add(timestamp, utc_offset);
	}
	return Value::TIMESTAMP(timestamp);
}

date_t PyDateTime::ToDate() {
	return Date::FromDate(year, month, day);
}
dtime_t PyDateTime::ToDuckTime() {
	return Time::FromTime(hour, minute, second, micros);
}

PyDate::PyDate(py::handle &ele) {
	auto ptr = ele.ptr();
	year = PyDateTime_GET_YEAR(ptr);
	month = PyDateTime_GET_MONTH(ptr);
	day = PyDateTime_GET_DAY(ptr);
}

Value PyDate::ToDuckValue() {
	return Value::DATE(year, month, day);
}

} // namespace duckdb
