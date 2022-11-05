#include "duckdb_python/python_objects.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

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

	// Timedelta stores any amount of seconds lower than a day only
	D_ASSERT(seconds < Interval::SECS_PER_DAY);

	// Convert overflow of days to months
	interval.months = days / Interval::DAYS_PER_MONTH;
	days -= interval.months * Interval::DAYS_PER_MONTH;

	microseconds += seconds * Interval::MICROS_PER_SEC;
	interval.days = days;
	interval.micros = microseconds;
	return interval;
}

PyDecimal::PyDecimal(py::handle &obj) : obj(obj) {
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
		if (scale > width) {
			// The value starts with 1 or more zeros, which are optimized out of the 'digits' array
			// 0.001 - width=1, exponent=-3
			width = scale + 1; // DECIMAL(4,3) - add 1 for the non-decimal values
		}
		if (width > Decimal::MAX_WIDTH_INT128) {
			type = LogicalType::DOUBLE;
			return true;
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
	default: // LCOV_EXCL_START
		throw NotImplementedException("case not implemented for type PyDecimalExponentType");
	} // LCOV_EXCL_STOP
	}
	return true;
}
// LCOV_EXCL_START
static void ExponentNotRecognized() {
	throw NotImplementedException("Failed to convert decimal.Decimal value, exponent type is unknown");
}
// LCOV_EXCL_STOP

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
	// LCOV_EXCL_START
	ExponentNotRecognized();
	// LCOV_EXCL_STOP
}

static bool WidthFitsInDecimal(int32_t width) {
	return width >= 0 && width <= Decimal::MAX_WIDTH_DECIMAL;
}

template <class OP>
Value PyDecimalCastSwitch(PyDecimal &decimal, uint8_t width, uint8_t scale) {
	if (width > DecimalWidth<int64_t>::max) {
		return OP::template Operation<hugeint_t>(decimal.signed_value, decimal.digits, width, scale);
	}
	if (width > DecimalWidth<int32_t>::max) {
		return OP::template Operation<int64_t>(decimal.signed_value, decimal.digits, width, scale);
	}
	if (width > DecimalWidth<int16_t>::max) {
		return OP::template Operation<int32_t>(decimal.signed_value, decimal.digits, width, scale);
	}
	return OP::template Operation<int16_t>(decimal.signed_value, decimal.digits, width, scale);
}

// Wont fit in a DECIMAL, fall back to DOUBLE
static Value CastToDouble(py::handle &obj) {
	string converted = py::str(obj);
	string_t decimal_string(converted);
	double double_val;
	bool try_cast = TryCast::Operation<string_t, double>(decimal_string, double_val, true);
	(void)try_cast;
	D_ASSERT(try_cast);
	return Value::DOUBLE(double_val);
}

Value PyDecimal::ToDuckValue() {
	int32_t width = digits.size();
	if (!WidthFitsInDecimal(width)) {
		return CastToDouble(obj);
	}
	switch (exponent_type) {
	case PyDecimalExponentType::EXPONENT_SCALE: {
		uint8_t scale = exponent_value;
		D_ASSERT(WidthFitsInDecimal(width));
		if (scale > width) {
			// Values like '0.001'
			width = scale + 1; // leave 1 room for the non-decimal value
		}
		if (!WidthFitsInDecimal(width)) {
			return CastToDouble(obj);
		}
		return PyDecimalCastSwitch<PyDecimalScaleConverter>(*this, width, scale);
	}
	case PyDecimalExponentType::EXPONENT_POWER: {
		uint8_t scale = exponent_value;
		width += scale;
		if (!WidthFitsInDecimal(width)) {
			return CastToDouble(obj);
		}
		return PyDecimalCastSwitch<PyDecimalPowerConverter>(*this, width, scale);
	}
	case PyDecimalExponentType::EXPONENT_NAN: {
		return Value::FLOAT(NAN);
	}
	case PyDecimalExponentType::EXPONENT_INFINITY: {
		return Value::FLOAT(INFINITY);
	}
	// LCOV_EXCL_START
	default: {
		throw NotImplementedException("case not implemented for type PyDecimalExponentType");
	} // LCOV_EXCL_STOP
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
		// 'Add' requires a date_t for overflows
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
		// Need to subtract the UTC offset, so we invert the interval
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
