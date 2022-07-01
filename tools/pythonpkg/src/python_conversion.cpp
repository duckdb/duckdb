#include "duckdb_python/python_conversion.hpp"
#include "duckdb_python/pybind_wrapper.hpp"

#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "datetime.h" //From Python

namespace duckdb {

PyDictionary::PyDictionary(py::object dict) {
	keys = py::list(dict.attr("keys")());
	values = py::list(dict.attr("values")());
	len = py::len(keys);
	this->dict = move(dict);
}

struct PyTimeDelta {
public:
	PyTimeDelta(py::handle &obj) {
		auto ptr = obj.ptr();
		days = PyDateTime_TIMEDELTA_GET_DAYS(ptr);
		seconds = PyDateTime_TIMEDELTA_GET_SECONDS(ptr);
		microseconds = PyDateTime_TIMEDELTA_GET_MICROSECONDS(ptr);
	}
	uint64_t days;
	uint32_t seconds;
	uint32_t microseconds;

public:
	interval_t ToInterval() {
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
};

struct PyDateTime {
public:
	PyDateTime(py::handle &obj) : obj(obj) {
		auto ptr = obj.ptr();
		year = PyDateTime_GET_YEAR(ptr);
		month = PyDateTime_GET_MONTH(ptr);
		day = PyDateTime_GET_DAY(ptr);
		hour = PyDateTime_DATE_GET_HOUR(ptr);
		minute = PyDateTime_DATE_GET_MINUTE(ptr);
		second = PyDateTime_DATE_GET_SECOND(ptr);
		micros = PyDateTime_DATE_GET_MICROSECOND(ptr);
		auto timezone = PyDateTime_DATE_GET_TZINFO(ptr);
		if (timezone != Py_None) {
			AdjustToUTC(timezone);
		}
	}
	py::handle &obj;
	int32_t year;
	int32_t month;
	int32_t day;
	int32_t hour;
	int32_t minute;
	int32_t second;
	int32_t micros;

public:
	void AdjustToUTC(PyObject *timezone) {
		auto tzinfo = py::object(timezone, true);
		//! Get the timedelta of the difference
		auto offset = tzinfo.attr("utcoffset")(obj);
	}
	date_t ToDate() {
		return Date::FromDate(year, month, day);
	}
	dtime_t ToTime() {
		return Time::FromTime(hour, minute, second, micros);
	}
	Value ToTimestamp() {
		return Value::TIMESTAMP(ToDate(), ToTime());
	}
};

Value TransformListValue(py::handle ele);

static Value EmptyMapValue() {
	return Value::MAP(Value::EMPTYLIST(LogicalType::SQLNULL), Value::EMPTYLIST(LogicalType::SQLNULL));
}

bool DictionaryHasMapFormat(const PyDictionary &dict) {
	if (dict.len != 2) {
		return false;
	}
	auto keys = dict.values.attr("__getitem__")(0);
	auto values = dict.values.attr("__getitem__")(1);
	// Dont check for 'py::list' to allow ducktyping
	if (!py::hasattr(keys, "__getitem__") || !py::hasattr(keys, "__len__")) {
		// throw std::runtime_error("Dictionary malformed, keys(index 0) found within 'dict.values' is not a list");
		return false;
	}
	if (!py::hasattr(values, "__getitem__") || !py::hasattr(values, "__len__")) {
		// throw std::runtime_error("Dictionary malformed, values(index 1) found within 'dict.values' is not a list");
		return false;
	}
	auto size = py::len(keys);
	if (size != py::len(values)) {
		// throw std::runtime_error("Dictionary malformed, keys and values lists are not of the same size");
		return false;
	}
	return true;
}

vector<string> TransformStructKeys(py::handle keys, idx_t size, const LogicalType &type) {
	vector<string> res;
	if (type.id() == LogicalTypeId::STRUCT) {
		auto &struct_keys = StructType::GetChildTypes(type);
		res.reserve(struct_keys.size());
		for (idx_t i = 0; i < struct_keys.size(); i++) {
			res.push_back(struct_keys[i].first);
		}
		return res;
	}
	res.reserve(size);
	for (idx_t i = 0; i < size; i++) {
		res.emplace_back(py::str(keys.attr("__getitem__")(i)));
	}
	return res;
}

Value TransformDictionaryToStruct(const PyDictionary &dict, const LogicalType &target_type = LogicalType::UNKNOWN) {
	auto struct_keys = TransformStructKeys(dict.keys, dict.len, target_type);

	child_list_t<Value> struct_values;
	for (idx_t i = 0; i < dict.len; i++) {
		auto val = TransformPythonValue(dict.values.attr("__getitem__")(i));
		struct_values.emplace_back(make_pair(move(struct_keys[i]), move(val)));
	}
	return Value::STRUCT(move(struct_values));
}

Value TransformStructFormatDictionaryToMap(const PyDictionary &dict) {
	if (dict.len == 0) {
		return EmptyMapValue();
	}
	auto keys = TransformListValue(dict.keys);
	auto values = TransformListValue(dict.values);
	return Value::MAP(move(keys), move(values));
}

Value TransformDictionaryToMap(const PyDictionary &dict, const LogicalType &target_type = LogicalType::UNKNOWN) {
	if (target_type.id() != LogicalTypeId::UNKNOWN && !DictionaryHasMapFormat(dict)) {
		// dict == { 'k1': v1, 'k2': v2, ..., 'kn': vn }
		return TransformStructFormatDictionaryToMap(dict);
	}

	auto keys = dict.values.attr("__getitem__")(0);
	auto values = dict.values.attr("__getitem__")(1);

	auto key_size = py::len(keys);
	D_ASSERT(key_size == py::len(values));
	if (key_size == 0) {
		// dict == { 'key': [], 'value': [] }
		return EmptyMapValue();
	}
	// dict == { 'key': [ ... ], 'value' : [ ... ] }
	auto key_list = TransformPythonValue(keys);
	auto value_list = TransformPythonValue(values);
	return Value::MAP(key_list, value_list);
}

Value TransformListValue(py::handle ele) {
	auto size = py::len(ele);

	if (size == 0) {
		return Value::EMPTYLIST(LogicalType::SQLNULL);
	}

	vector<Value> values;
	values.reserve(size);

	LogicalType element_type = LogicalType::SQLNULL;
	for (idx_t i = 0; i < size; i++) {
		Value new_value = TransformPythonValue(ele.attr("__getitem__")(i));
		element_type = LogicalType::MaxLogicalType(element_type, new_value.type());
		values.push_back(move(new_value));
	}

	return Value::LIST(element_type, values);
}

Value TransformDictionary(const PyDictionary &dict) {
	//! DICT -> MAP FORMAT
	// keys() = [key, value]
	// values() = [ [n keys] ], [ [n values] ]

	//! DICT -> STRUCT FORMAT
	// keys() = ['a', .., 'n']
	// values() = [ val1, .., valn]
	if (dict.len == 0) {
		// dict == {}
		return EmptyMapValue();
	}

	if (DictionaryHasMapFormat(dict)) {
		return TransformDictionaryToMap(dict);
	}
	return TransformDictionaryToStruct(dict);
}

Value TransformPythonValue(py::handle ele, const LogicalType &target_type) {
	auto &import_cache = *DuckDBPyConnection::ImportCache();

	if (ele.is_none()) {
		return Value();
	} else if (py::isinstance<py::bool_>(ele)) {
		return Value::BOOLEAN(ele.cast<bool>());
	} else if (py::isinstance<py::int_>(ele)) {
		return Value::BIGINT(ele.cast<int64_t>());
	} else if (py::isinstance<py::float_>(ele)) {
		if (std::isnan(PyFloat_AsDouble(ele.ptr()))) {
			return Value();
		}
		return Value::DOUBLE(ele.cast<double>());
	} else if (py::isinstance(ele, import_cache.decimal.Decimal())) {
		return py::str(ele).cast<string>();
	} else if (py::isinstance(ele, import_cache.uuid.UUID())) {
		auto string_val = py::str(ele).cast<string>();
		return Value::UUID(string_val);
	} else if (py::isinstance(ele, import_cache.datetime.datetime())) {
		auto datetime = PyDateTime(ele);
		return datetime.ToTimestamp();
	} else if (py::isinstance(ele, import_cache.datetime.time())) {
		auto ptr = ele.ptr();
		auto hour = PyDateTime_TIME_GET_HOUR(ptr);
		auto minute = PyDateTime_TIME_GET_MINUTE(ptr);
		auto second = PyDateTime_TIME_GET_SECOND(ptr);
		auto micros = PyDateTime_TIME_GET_MICROSECOND(ptr);
		auto tzinfo = PyDateTime_TIME_GET_TZINFO(ptr);
		// if (tzinfo != Py_None) {
		//	return Value::TIMETZ(Time::FromTime(hour, minute, second, micros));
		// }
		return Value::TIME(hour, minute, second, micros);
	} else if (py::isinstance(ele, import_cache.datetime.date())) {
		auto ptr = ele.ptr();
		auto year = PyDateTime_GET_YEAR(ptr);
		auto month = PyDateTime_GET_MONTH(ptr);
		auto day = PyDateTime_GET_DAY(ptr);
		return Value::DATE(year, month, day);
	} else if (py::isinstance(ele, import_cache.datetime.timedelta())) {
		auto timedelta = PyTimeDelta(ele);
		return Value::INTERVAL(timedelta.ToInterval());
	} else if (py::isinstance<py::str>(ele)) {
		return ele.cast<string>();
	} else if (py::isinstance<py::bytearray>(ele)) {
		auto byte_array = ele.ptr();
		auto bytes = PyByteArray_AsString(byte_array);
		return Value::BLOB_RAW(bytes);
	} else if (py::isinstance<py::memoryview>(ele)) {
		py::memoryview py_view = ele.cast<py::memoryview>();
		PyObject *py_view_ptr = py_view.ptr();
		Py_buffer *py_buf = PyMemoryView_GET_BUFFER(py_view_ptr);
		return Value::BLOB(const_data_ptr_t(py_buf->buf), idx_t(py_buf->len));
	} else if (py::isinstance<py::bytes>(ele)) {
		const string &ele_string = ele.cast<string>();
		return Value::BLOB(const_data_ptr_t(ele_string.data()), ele_string.size());
	} else if (py::isinstance<py::list>(ele)) {
		return TransformListValue(ele);
	} else if (py::isinstance<py::dict>(ele)) {
		PyDictionary dict = PyDictionary(py::object(ele, true));
		switch (target_type.id()) {
		case LogicalTypeId::STRUCT:
			return TransformDictionaryToStruct(dict, target_type);
		case LogicalTypeId::MAP:
			return TransformDictionaryToMap(dict, target_type);
		default:
			return TransformDictionary(dict);
		}
	} else if (py::isinstance(ele, import_cache.numpy.ndarray())) {
		return TransformPythonValue(ele.attr("tolist")());
	} else {
		throw std::runtime_error("TransformPythonValue unknown param type " + py::str(ele.get_type()).cast<string>());
	}
}

} // namespace duckdb
