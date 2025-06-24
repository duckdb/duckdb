#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/pyutil.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/main/client_properties.hpp"

#include "datetime.h" //from python

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

namespace duckdb {

struct PyDictionary {
public:
	PyDictionary(py::object dict);
	// These are cached so we don't have to create new objects all the time
	// The CPython API offers PyDict_Keys but that creates a new reference every time, same for values
	py::object keys;
	py::object values;
	idx_t len;

public:
	py::handle operator[](const py::object &obj) const {
		return PyDict_GetItem(dict.ptr(), obj.ptr());
	}

public:
	string ToString() const {
		return string(py::str(dict));
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

	struct PyDecimalScaleConverter {
		template <typename T, typename = std::enable_if<std::numeric_limits<T>::is_integer, T>>
		static Value Operation(bool signed_value, vector<uint8_t> &digits, uint8_t width, uint8_t scale) {
			T value = 0;
			for (auto it = digits.begin(); it != digits.end(); it++) {
				value = value * 10 + *it;
			}
			if (signed_value) {
				value = -value;
			}
			return Value::DECIMAL(value, width, scale);
		}
	};

	struct PyDecimalPowerConverter {
		template <typename T, typename = std::enable_if<std::numeric_limits<T>::is_integer, T>>
		static Value Operation(bool signed_value, vector<uint8_t> &digits, uint8_t width, uint8_t scale) {
			T value = 0;
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
	};

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
	py::handle &obj;
};

struct PyTimeDelta {
public:
	PyTimeDelta(py::handle &obj);
	int32_t days;
	int32_t seconds;
	int64_t microseconds;

public:
	interval_t ToInterval();

private:
	static int64_t GetDays(py::handle &obj);
	static int64_t GetSeconds(py::handle &obj);
	static int64_t GetMicros(py::handle &obj);
};

struct PyTime {
public:
	PyTime(py::handle &obj);
	py::handle &obj;
	int32_t hour;
	int32_t minute;
	int32_t second;
	int32_t microsecond;
	py::object timezone_obj;

public:
	dtime_t ToDuckTime();
	Value ToDuckValue();

private:
	static int32_t GetHours(py::handle &obj);
	static int32_t GetMinutes(py::handle &obj);
	static int32_t GetSeconds(py::handle &obj);
	static int32_t GetMicros(py::handle &obj);
	static py::object GetTZInfo(py::handle &obj);
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
	py::object tzone_obj;

public:
	timestamp_t ToTimestamp();
	date_t ToDate();
	dtime_t ToDuckTime();
	Value ToDuckValue(const LogicalType &target_type);

public:
	static int32_t GetYears(py::handle &obj);
	static int32_t GetMonths(py::handle &obj);
	static int32_t GetDays(py::handle &obj);
	static int32_t GetHours(py::handle &obj);
	static int32_t GetMinutes(py::handle &obj);
	static int32_t GetSeconds(py::handle &obj);
	static int32_t GetMicros(py::handle &obj);
	static py::object GetTZInfo(py::handle &obj);
};

struct PyDate {
public:
	PyDate(py::handle &ele);
	int32_t year;
	int32_t month;
	int32_t day;

public:
	date_t ToDate();
	Value ToDuckValue();
};

struct PyTimezone {
public:
	PyTimezone() = delete;

public:
	DUCKDB_API static int32_t GetUTCOffsetSeconds(py::handle &tzone_obj);
	DUCKDB_API static interval_t GetUTCOffset(py::handle &datetime, py::handle &tzone_obj);
};

struct PythonObject {
	static void Initialize();
	static py::object FromStruct(const Value &value, const LogicalType &id, const ClientProperties &client_properties);
	static py::object FromValue(const Value &value, const LogicalType &id, const ClientProperties &client_properties);
};

template <class T>
class Optional : public py::object {
public:
	Optional(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool check_(const py::handle &object) {
		return object.is_none() || py::isinstance<T>(object);
	}
};

class FileLikeObject : public py::object {
public:
	FileLikeObject(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool check_(const py::handle &object) {
		return py::isinstance(object, py::module::import("io").attr("IOBase"));
	}
};

} // namespace duckdb

namespace pybind11 {
namespace detail {
template <typename T>
struct handle_type_name<duckdb::Optional<T>> {
	static constexpr auto name = const_name("typing.Optional[") + concat(make_caster<T>::name) + const_name("]");
};
} // namespace detail
} // namespace pybind11
