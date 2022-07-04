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
	py::object keys;
	py::object values;
	idx_t len;

private:
	py::object dict;
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
	PyObject *timezone;

public:
	dtime_t ToTime();
	Value ToValue();
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
	PyObject *timezone;

public:
	timestamp_t ToTimestamp();
	date_t ToDate();
	dtime_t ToTime();
	Value ToValue();
};

struct PyDate {
public:
	PyDate(py::handle &ele);
	int32_t year;
	int32_t month;
	int32_t day;

public:
	Value ToValue();
};

struct PyTimezone {
public:
	PyTimezone() = delete;

public:
	static interval_t GetUTCOffset(PyObject *timezone);
};

} // namespace duckdb
