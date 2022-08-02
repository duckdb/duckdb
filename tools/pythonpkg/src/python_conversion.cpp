#include "duckdb_python/python_conversion.hpp"
#include "duckdb_python/pybind_wrapper.hpp"

#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"

#include "datetime.h" //From Python

#include <limits>

namespace duckdb {

Value TransformListValue(py::handle ele);

static Value EmptyMapValue() {
	return Value::MAP(Value::EMPTYLIST(LogicalType::SQLNULL), Value::EMPTYLIST(LogicalType::SQLNULL));
}

vector<string> TransformStructKeys(py::handle keys, idx_t size, const LogicalType &type = LogicalType::UNKNOWN) {
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

bool DictionaryHasMapFormat(const PyDictionary &dict) {
	if (dict.len != 2) {
		return false;
	}

	//{ 'key': [ .. keys .. ], 'value': [ .. values .. ]}
	auto keys_key = py::str("key");
	auto values_key = py::str("value");
	auto keys = dict[keys_key];
	auto values = dict[values_key];
	if (!keys || !values) {
		return false;
	}

	// Dont check for 'py::list' to allow ducktyping
	if (!py::hasattr(keys, "__getitem__") || !py::hasattr(keys, "__len__")) {
		return false;
	}
	if (!py::hasattr(values, "__getitem__") || !py::hasattr(values, "__len__")) {
		return false;
	}
	auto size = py::len(keys);
	if (size != py::len(values)) {
		return false;
	}
	return true;
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

bool TryTransformPythonIntegerToDouble(Value &res, py::handle ele) {
	double number = PyLong_AsDouble(ele.ptr());
	if (number == -1.0 && PyErr_Occurred()) {
		PyErr_Clear();
		return false;
	}
	res = Value::DOUBLE(number);
	return true;
}

void TransformPythonUnsigned(uint64_t value, Value &res) {
	if (value > (uint64_t)std::numeric_limits<uint32_t>::max()) {
		res = Value::UBIGINT(value);
	} else if (value > (int64_t)std::numeric_limits<uint16_t>::max()) {
		res = Value::UINTEGER(value);
	} else if (value > (int64_t)std::numeric_limits<uint16_t>::max()) {
		res = Value::USMALLINT(value);
	} else {
		res = Value::UTINYINT(value);
	}
}

// TODO: add support for HUGEINT
bool TryTransformPythonNumeric(Value &res, py::handle ele) {
	auto ptr = ele.ptr();

	int overflow;
	int64_t value = PyLong_AsLongLongAndOverflow(ptr, &overflow);
	if (overflow == -1) {
		PyErr_Clear();
		return TryTransformPythonIntegerToDouble(res, ele);
	} else if (overflow == 1) {
		uint64_t unsigned_value = PyLong_AsUnsignedLongLong(ptr);
		if (PyErr_Occurred()) {
			PyErr_Clear();
			return TryTransformPythonIntegerToDouble(res, ele);
		} else {
			TransformPythonUnsigned(unsigned_value, res);
		}
		PyErr_Clear();
		return true;
	} else if (value == -1 && PyErr_Occurred()) {
		return false;
	}

	if (value < (int64_t)std::numeric_limits<int32_t>::min() || value > (int64_t)std::numeric_limits<int32_t>::max()) {
		res = Value::BIGINT(value);
	} else if (value < (int32_t)std::numeric_limits<int16_t>::min() ||
	           value > (int32_t)std::numeric_limits<int16_t>::max()) {
		res = Value::INTEGER(value);
	} else if (value < (int16_t)std::numeric_limits<int8_t>::min() ||
	           value > (int16_t)std::numeric_limits<int8_t>::max()) {
		res = Value::SMALLINT(value);
	} else {
		res = Value::TINYINT(value);
	}
	return true;
}

Value TransformPythonValue(py::handle ele, const LogicalType &target_type) {
	auto &import_cache = *DuckDBPyConnection::ImportCache();

	if (ele.is_none()) {
		return Value();
	} else if (py::isinstance<py::bool_>(ele)) {
		return Value::BOOLEAN(ele.cast<bool>());
	} else if (py::isinstance<py::int_>(ele)) {
		Value integer;
		if (!TryTransformPythonNumeric(integer, ele)) {
			throw InvalidInputException("An error occurred attempting to convert a python integer");
		}
		return integer;
	} else if (py::isinstance<py::float_>(ele)) {
		if (std::isnan(PyFloat_AsDouble(ele.ptr()))) {
			return Value();
		}
		return Value::DOUBLE(ele.cast<double>());
	} else if (py::isinstance(ele, import_cache.decimal.Decimal())) {
		PyDecimal decimal(ele);
		return decimal.ToDuckValue();
	} else if (py::isinstance(ele, import_cache.uuid.UUID())) {
		auto string_val = py::str(ele).cast<string>();
		return Value::UUID(string_val);
	} else if (py::isinstance(ele, import_cache.datetime.datetime())) {
		auto datetime = PyDateTime(ele);
		return datetime.ToDuckValue();
	} else if (py::isinstance(ele, import_cache.datetime.time())) {
		auto time = PyTime(ele);
		return time.ToDuckValue();
	} else if (py::isinstance(ele, import_cache.datetime.date())) {
		auto date = PyDate(ele);
		return date.ToDuckValue();
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
		PyDictionary dict = PyDictionary(py::reinterpret_borrow<py::object>(ele));
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
		throw NotImplementedException("Unable to transform python value of type '%s' to DuckDB LogicalType",
		                              py::str(ele.get_type()).cast<string>());
	}
}

} // namespace duckdb
