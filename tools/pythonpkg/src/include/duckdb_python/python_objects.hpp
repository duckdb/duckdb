#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"

#include "datetime.h" //from python

namespace duckdb {

/* Backport for Python < 3.10 */
#if PY_VERSION_HEX < 0x030a00a1
#ifndef PyDateTime_TIME_GET_TZINFO
#define PyDateTime_TIME_GET_TZINFO(o) ((((PyDateTime_Time *)o)->hastzinfo) ? ((PyDateTime_Time *)o)->tzinfo : Py_None)
#endif
#ifndef PyDateTime_DATE_GET_TZINFO
#define PyDateTime_DATE_GET_TZINFO(o)                                                                                  \
	((((PyDateTime_DateTime *)o)->hastzinfo) ? ((PyDateTime_DateTime *)o)->tzinfo : Py_None)
#endif
#endif

#define PyDateTime_TIMEDELTA_GET_DAYS(o)         (((PyDateTime_Delta *)(o))->days)
#define PyDateTime_TIMEDELTA_GET_SECONDS(o)      (((PyDateTime_Delta *)(o))->seconds)
#define PyDateTime_TIMEDELTA_GET_MICROSECONDS(o) (((PyDateTime_Delta *)(o))->microseconds)

struct PyDictionary {
public:
	PyDictionary(py::object dict);
	// FIXME: should probably remove these, as they aren't used if the dictionary has MAP format
	py::object keys;
	py::object values;
	idx_t len;

public:
	PyObject *operator[](const py::object &obj) const {
		return PyDict_GetItem(dict.ptr(), obj.ptr());
	}

private:
	py::object dict;
};

enum class PyDecimalExponentType {
	EXPONENT_SCALE,    //! Amount of digits after the decimal point
	EXPONENT_POWER,    //! How many zeros behind the decimal point
	EXPONENT_INFINITY, //! Decimal is INFINITY
	EXPONENT_NAN       //! Decimal is NAN
};

struct PyDecimal {
public:
	PyDecimal(py::handle &obj);
	vector<uint8_t> digits;
	bool signed_value = false;

	PyDecimalExponentType exponent_type;
	int32_t exponent_value;

public:
	bool TryGetType(LogicalType &type);
	Value ToDuckValue();

private:
	void SetExponent(py::handle &exponent);
};

struct PyTimeDelta {
public:
	PyTimeDelta(py::handle &obj);
	int64_t days;
	int64_t seconds;
	int64_t microseconds;

public:
	interval_t ToInterval();
};

struct PyTime {
public:
	PyTime(py::handle &obj);
	py::handle &obj;
	int32_t hour;
	int32_t minute;
	int32_t second;
	int32_t microsecond;
	PyObject *timezone_obj;

public:
	dtime_t ToDuckTime();
	Value ToDuckValue();
};

struct PyDateTime {
public:
	PyDateTime(py::handle &obj);
	py::handle &obj;
	int32_t year;
	int32_t month;
	int32_t day;
	int32_t hour;
	int32_t minute;
	int32_t second;
	int32_t micros;
	PyObject *tzone_obj;

public:
	timestamp_t ToTimestamp();
	date_t ToDate();
	dtime_t ToDuckTime();
	Value ToDuckValue();
};

struct PyDate {
public:
	PyDate(py::handle &ele);
	int32_t year;
	int32_t month;
	int32_t day;

public:
	Value ToDuckValue();
};

struct PyTimezone {
public:
	PyTimezone() = delete;

public:
	DUCKDB_API static interval_t GetUTCOffset(PyObject *tzone_obj);
};

} // namespace duckdb
