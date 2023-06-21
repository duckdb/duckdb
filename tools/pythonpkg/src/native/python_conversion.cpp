#include "duckdb_python/python_conversion.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/common/types.hpp"

#include "datetime.h" //From Python

#include <limits>

namespace duckdb {

Value TransformListValue(py::handle ele);

static Value EmptyMapValue() {
	auto map_type = LogicalType::MAP(LogicalType::SQLNULL, LogicalType::SQLNULL);
	return Value::MAP(ListType::GetChildType(map_type), vector<Value>());
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

	bool struct_target = target_type.id() == LogicalTypeId::STRUCT;
	if (struct_target) {
		D_ASSERT(dict.len == StructType::GetChildCount(target_type));
	}

	child_list_t<Value> struct_values;
	for (idx_t i = 0; i < dict.len; i++) {
		auto &child_type = struct_target ? StructType::GetChildType(target_type, i) : LogicalType::UNKNOWN;
		auto val = TransformPythonValue(dict.values.attr("__getitem__")(i), child_type);
		struct_values.emplace_back(make_pair(std::move(struct_keys[i]), std::move(val)));
	}
	return Value::STRUCT(std::move(struct_values));
}

Value TransformStructFormatDictionaryToMap(const PyDictionary &dict) {
	if (dict.len == 0) {
		return EmptyMapValue();
	}

	auto size = py::len(dict.keys);
	D_ASSERT(size == py::len(dict.values));

	LogicalType key_type = LogicalType::SQLNULL;
	LogicalType value_type = LogicalType::SQLNULL;

	vector<Value> elements;
	for (idx_t i = 0; i < size; i++) {

		Value new_key = TransformPythonValue(dict.keys.attr("__getitem__")(i));
		Value new_value = TransformPythonValue(dict.values.attr("__getitem__")(i));

		key_type = LogicalType::MaxLogicalType(key_type, new_key.type());
		value_type = LogicalType::MaxLogicalType(value_type, new_value.type());

		child_list_t<Value> struct_values;
		struct_values.emplace_back(make_pair("key", std::move(new_key)));
		struct_values.emplace_back(make_pair("value", std::move(new_value)));

		elements.push_back(Value::STRUCT(std::move(struct_values)));
	}

	LogicalType map_type = LogicalType::MAP(key_type, value_type);

	return Value::MAP(ListType::GetChildType(map_type), std::move(elements));
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

	LogicalType key_type = LogicalType::SQLNULL;
	LogicalType value_type = LogicalType::SQLNULL;

	vector<Value> elements;
	for (idx_t i = 0; i < key_size; i++) {

		Value new_key = ListValue::GetChildren(key_list)[i];
		Value new_value = ListValue::GetChildren(value_list)[i];

		key_type = LogicalType::MaxLogicalType(key_type, new_key.type());
		value_type = LogicalType::MaxLogicalType(value_type, new_value.type());

		child_list_t<Value> struct_values;
		struct_values.emplace_back(make_pair("key", std::move(new_key)));
		struct_values.emplace_back(make_pair("value", std::move(new_value)));

		elements.push_back(Value::STRUCT(std::move(struct_values)));
	}

	LogicalType map_type = LogicalType::MAP(key_type, value_type);

	return Value::MAP(ListType::GetChildType(map_type), std::move(elements));
}

Value TransformListValue(py::handle ele, const LogicalType &target_type = LogicalType::UNKNOWN) {
	auto size = py::len(ele);

	if (size == 0) {
		return Value::EMPTYLIST(LogicalType::SQLNULL);
	}

	vector<Value> values;
	values.reserve(size);

	bool list_target = target_type.id() == LogicalTypeId::LIST;

	LogicalType element_type = LogicalType::SQLNULL;
	for (idx_t i = 0; i < size; i++) {
		auto &child_type = list_target ? ListType::GetChildType(target_type) : LogicalType::UNKNOWN;
		Value new_value = TransformPythonValue(ele.attr("__getitem__")(i), child_type);
		element_type = LogicalType::MaxLogicalType(element_type, new_value.type());
		values.push_back(std::move(new_value));
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
bool TryTransformPythonNumeric(Value &res, py::handle ele, const LogicalType &target_type) {
	auto ptr = ele.ptr();

	int overflow;
	int64_t value = PyLong_AsLongLongAndOverflow(ptr, &overflow);
	if (overflow == -1) {
		PyErr_Clear();
		if (target_type.id() == LogicalTypeId::BIGINT) {
			throw InvalidInputException(StringUtil::Format("Failed to cast value: Python value '%s' to INT64",
			                                               std::string(pybind11::str(ele))));
		}
		return TryTransformPythonIntegerToDouble(res, ele);
	} else if (overflow == 1) {
		if (target_type.InternalType() == PhysicalType::INT64) {
			throw InvalidInputException(StringUtil::Format("Failed to cast value: Python value '%s' to INT64",
			                                               std::string(pybind11::str(ele))));
		}
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

PythonObjectType GetPythonObjectType(py::handle &ele) {
	auto &import_cache = *DuckDBPyConnection::ImportCache();

	if (ele.is_none()) {
		return PythonObjectType::None;
	} else if (py::isinstance(ele, import_cache.pandas().libs.NAType())) {
		return PythonObjectType::None;
	} else if (py::isinstance<py::bool_>(ele)) {
		return PythonObjectType::Bool;
	} else if (py::isinstance<py::int_>(ele)) {
		return PythonObjectType::Integer;
	} else if (py::isinstance<py::float_>(ele)) {
		return PythonObjectType::Float;
	} else if (py::isinstance(ele, import_cache.decimal().Decimal())) {
		return PythonObjectType::Decimal;
	} else if (py::isinstance(ele, import_cache.uuid().UUID())) {
		return PythonObjectType::Uuid;
	} else if (py::isinstance(ele, import_cache.datetime().datetime())) {
		return PythonObjectType::Datetime;
	} else if (py::isinstance(ele, import_cache.datetime().time())) {
		return PythonObjectType::Time;
	} else if (py::isinstance(ele, import_cache.datetime().date())) {
		return PythonObjectType::Date;
	} else if (py::isinstance(ele, import_cache.datetime().timedelta())) {
		return PythonObjectType::Timedelta;
	} else if (py::isinstance<py::str>(ele)) {
		return PythonObjectType::String;
	} else if (py::isinstance<py::bytearray>(ele)) {
		return PythonObjectType::ByteArray;
	} else if (py::isinstance<py::memoryview>(ele)) {
		return PythonObjectType::MemoryView;
	} else if (py::isinstance<py::bytes>(ele)) {
		return PythonObjectType::Bytes;
	} else if (py::isinstance<py::list>(ele) || py::isinstance<py::tuple>(ele)) {
		return PythonObjectType::List;
	} else if (py::isinstance<py::dict>(ele)) {
		return PythonObjectType::Dict;
	} else if (py::isinstance(ele, import_cache.numpy().ndarray())) {
		return PythonObjectType::NdArray;
	} else if (py::isinstance(ele, import_cache.numpy().datetime64())) {
		return PythonObjectType::NdDatetime;
	} else {
		return PythonObjectType::Other;
	}
}

Value TransformPythonValue(py::handle ele, const LogicalType &target_type, bool nan_as_null) {
	auto object_type = GetPythonObjectType(ele);

	switch (object_type) {
	case PythonObjectType::None:
		return Value();
	case PythonObjectType::Bool:
		return Value::BOOLEAN(ele.cast<bool>());
	case PythonObjectType::Integer: {
		Value integer;
		if (!TryTransformPythonNumeric(integer, ele, target_type)) {
			throw InvalidInputException("An error occurred attempting to convert a python integer");
		}
		return integer;
	}
	case PythonObjectType::Float:
		if (nan_as_null && std::isnan(PyFloat_AsDouble(ele.ptr()))) {
			return Value();
		}
		return Value::DOUBLE(ele.cast<double>());
	case PythonObjectType::Decimal: {
		PyDecimal decimal(ele);
		return decimal.ToDuckValue();
	}
	case PythonObjectType::Uuid: {
		auto string_val = py::str(ele).cast<string>();
		return Value::UUID(string_val);
	}
	case PythonObjectType::Datetime: {
		auto &import_cache = *DuckDBPyConnection::ImportCache();
		bool is_nat = false;
		if (import_cache.pandas().isnull.IsLoaded()) {
			auto isnull_result = import_cache.pandas().isnull()(ele);
			is_nat = string(py::str(isnull_result)) == "True";
		}
		if (is_nat) {
			return Value();
		}
		auto datetime = PyDateTime(ele);
		return datetime.ToDuckValue();
	}
	case PythonObjectType::Time: {
		auto time = PyTime(ele);
		return time.ToDuckValue();
	}
	case PythonObjectType::Date: {
		auto date = PyDate(ele);
		return date.ToDuckValue();
	}
	case PythonObjectType::Timedelta: {
		auto timedelta = PyTimeDelta(ele);
		return Value::INTERVAL(timedelta.ToInterval());
	}
	case PythonObjectType::String:
		return ele.cast<string>();
	case PythonObjectType::ByteArray: {
		auto byte_array = ele.ptr();
		const_data_ptr_t bytes = const_data_ptr_cast(PyByteArray_AsString(byte_array)); // NOLINT
		idx_t byte_length = PyUtil::PyByteArrayGetSize(byte_array);                     // NOLINT
		return Value::BLOB(bytes, byte_length);
	}
	case PythonObjectType::MemoryView: {
		py::memoryview py_view = ele.cast<py::memoryview>();
		PyObject *py_view_ptr = py_view.ptr();
		Py_buffer *py_buf = PyUtil::PyMemoryViewGetBuffer(py_view_ptr); // NOLINT
		return Value::BLOB(const_data_ptr_t(py_buf->buf), idx_t(py_buf->len));
	}
	case PythonObjectType::Bytes: {
		const string &ele_string = ele.cast<string>();
		return Value::BLOB(const_data_ptr_t(ele_string.data()), ele_string.size());
	}
	case PythonObjectType::List:
		return TransformListValue(ele, target_type);
	case PythonObjectType::Dict: {
		PyDictionary dict = PyDictionary(py::reinterpret_borrow<py::object>(ele));
		switch (target_type.id()) {
		case LogicalTypeId::STRUCT:
			return TransformDictionaryToStruct(dict, target_type);
		case LogicalTypeId::MAP:
			return TransformDictionaryToMap(dict, target_type);
		default:
			return TransformDictionary(dict);
		}
	}
	case PythonObjectType::NdArray:
	case PythonObjectType::NdDatetime:
		return TransformPythonValue(ele.attr("tolist")(), target_type, nan_as_null);
	case PythonObjectType::Other:
		throw NotImplementedException("Unable to transform python value of type '%s' to DuckDB LogicalType",
		                              py::str(ele.get_type()).cast<string>());
	}
}

} // namespace duckdb
