//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyresult.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "array_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/types.hpp"

#include "datetime.h" // from Python

namespace duckdb {

struct PyDictionary {
public:
	PyDictionary(py::object dict);
	py::object keys;
	py::object values;
	idx_t len;

private:
	py::object dict;
};

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

bool DictionaryHasMapFormat(const PyDictionary &dict);
Value TransformPythonValue(py::handle ele, const LogicalType &target_type = LogicalType::UNKNOWN);

} // namespace duckdb
