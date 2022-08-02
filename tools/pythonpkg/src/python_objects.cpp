#include "duckdb_python/python_objects.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

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

	py::object exponent = as_tuple.attr("exponent");
	SetExponent(exponent);

	auto sign = py::cast<int8_t>(as_tuple.attr("sign"));
	signed_value = sign != 0;

	auto decimal_digits = as_tuple.attr("digits");
	auto width = py::len(decimal_digits);
	digits.reserve(width);
	for (auto digit : decimal_digits) {
		digits.push_back(py::cast<uint8_t>(digit));
	}
}

bool PyDecimal::TryGetType(LogicalType &type) {
	int32_t width = digits.size();

	switch (exponent_type) {
	case PyDecimalExponentType::EXPONENT_SCALE: {
	case PyDecimalExponentType::EXPONENT_POWER: {
		auto scale = exponent_value;
		if (exponent_type == PyDecimalExponentType::EXPONENT_POWER) {
			width += scale;
		}
		if (width > Decimal::MAX_WIDTH_INT64) {
			return false;
		}
		type = LogicalType::DECIMAL(width, scale);
		return true;
	}
	case PyDecimalExponentType::EXPONENT_INFINITY: {
		type = LogicalType::FLOAT;
		return true;
	}
	case PyDecimalExponentType::EXPONENT_NAN: {
		type = LogicalType::FLOAT;
		return true;
	}
	default:
		throw NotImplementedException("case not implemented for type PyDecimalExponentType");
	}
	}
	return true;
}

static void ExponentNotRecognized() {
	throw NotImplementedException("Failed to convert decimal.Decimal value, exponent type is unknown");
}

void PyDecimal::SetExponent(py::handle &exponent) {
	if (py::isinstance<py::int_>(exponent)) {
		this->exponent_value = py::cast<int32_t>(exponent);
		if (this->exponent_value >= 0) {
			exponent_type = PyDecimalExponentType::EXPONENT_POWER;
			return;
		}
		exponent_value *= -1;
		exponent_type = PyDecimalExponentType::EXPONENT_SCALE;
		return;
	}
	if (py::isinstance<py::str>(exponent)) {
		string exponent_string = py::str(exponent);
		if (exponent_string == "n") {
			exponent_type = PyDecimalExponentType::EXPONENT_NAN;
			return;
		}
		if (exponent_string == "F") {
			exponent_type = PyDecimalExponentType::EXPONENT_INFINITY;
			return;
		}
	}
	ExponentNotRecognized();
}

static void UnsupportedWidth(uint16_t width) {
	throw ConversionException(
	    "Failed to convert to a DECIMAL value with a width of %d because it exceeds the max supported with of %d",
	    width, Decimal::MAX_WIDTH_INT64);
}

Value PyDecimal::ToDuckValue() {
	int32_t width = digits.size();
	switch (exponent_type) {
	case PyDecimalExponentType::EXPONENT_SCALE: {
		uint8_t scale = exponent_value;
		if (width > Decimal::MAX_WIDTH_INT64) {
			UnsupportedWidth(width);
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
	case PyDecimalExponentType::EXPONENT_POWER: {
		uint8_t scale = exponent_value;
		width += scale;
		if (width > Decimal::MAX_WIDTH_INT64) {
			UnsupportedWidth(width);
		}
		int64_t value = 0;
		for (auto &digit : digits) {
			value = value * 10 + digit;
		}
		D_ASSERT(scale >= 0);
		int64_t multiplier =
		    NumericHelper::POWERS_OF_TEN[MinValue<uint8_t>(scale, NumericHelper::CACHED_POWERS_OF_TEN - 1)];
		for (auto power = scale; power > NumericHelper::CACHED_POWERS_OF_TEN; power--) {
			multiplier *= 10;
		}
		value *= multiplier;
		if (signed_value) {
			value = -value;
		}
		return Value::DECIMAL(value, width, scale);
	}
	case PyDecimalExponentType::EXPONENT_NAN: {
		return Value::FLOAT(NAN);
	}
	case PyDecimalExponentType::EXPONENT_INFINITY: {
		return Value::FLOAT(INFINITY);
	}
	default: {
		throw NotImplementedException("case not implemented for type PyDecimalExponentType");
	}
	}
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
