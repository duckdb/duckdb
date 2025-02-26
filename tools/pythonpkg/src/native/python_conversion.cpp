#include "duckdb_python/python_conversion.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

#include "datetime.h" //From Python

#include "duckdb/common/limits.hpp"

namespace duckdb {

Value TransformListValue(py::handle ele);

static Value EmptyMapValue() {
	auto map_type = LogicalType::MAP(LogicalType::SQLNULL, LogicalType::SQLNULL);
	return Value::MAP(ListType::GetChildType(map_type), vector<Value>());
}

vector<string> TransformStructKeys(py::handle keys, idx_t size, const LogicalType &type = LogicalType::UNKNOWN) {
	vector<string> res;
	res.reserve(size);
	for (idx_t i = 0; i < size; i++) {
		res.emplace_back(py::str(keys.attr("__getitem__")(i)));
	}
	return res;
}

static bool IsValidMapComponent(const py::handle &component) {
	// The component is either NULL
	if (py::none().is(component)) {
		return true;
	}
	if (!py::hasattr(component, "__getitem__")) {
		return false;
	}
	if (!py::hasattr(component, "__len__")) {
		return false;
	}
	return true;
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

	if (!IsValidMapComponent(keys)) {
		return false;
	}
	if (!IsValidMapComponent(values)) {
		return false;
	}

	// If either of the components is NULL, return early
	if (py::none().is(keys) || py::none().is(values)) {
		return true;
	}

	// Verify that both the keys and values are of the same length
	auto size = py::len(keys);
	if (size != py::len(values)) {
		return false;
	}
	return true;
}

Value TransformDictionaryToStruct(const PyDictionary &dict, const LogicalType &target_type = LogicalType::UNKNOWN) {
	auto struct_keys = TransformStructKeys(dict.keys, dict.len, target_type);

	bool struct_target = target_type.id() == LogicalTypeId::STRUCT;
	if (struct_target && dict.len != StructType::GetChildCount(target_type)) {
		throw InvalidInputException("We could not convert the object %s to the desired target type (%s)",
		                            dict.ToString(), target_type.ToString());
	}

	case_insensitive_map_t<idx_t> key_mapping;
	for (idx_t i = 0; i < struct_keys.size(); i++) {
		key_mapping[struct_keys[i]] = i;
	}

	child_list_t<Value> struct_values;
	for (idx_t i = 0; i < dict.len; i++) {
		auto &key = struct_target ? StructType::GetChildName(target_type, i) : struct_keys[i];
		auto value_index = key_mapping[key];
		auto &child_type = struct_target ? StructType::GetChildType(target_type, i) : LogicalType::UNKNOWN;
		auto val = TransformPythonValue(dict.values.attr("__getitem__")(value_index), child_type);
		struct_values.emplace_back(make_pair(std::move(key), std::move(val)));
	}
	return Value::STRUCT(std::move(struct_values));
}

Value TransformStructFormatDictionaryToMap(const PyDictionary &dict, const LogicalType &target_type) {
	if (dict.len == 0) {
		return EmptyMapValue();
	}

	if (target_type.id() != LogicalTypeId::MAP) {
		throw InvalidInputException("Please provide a valid target type for transform from Python to Value");
	}

	if (py::none().is(dict.keys) || py::none().is(dict.values)) {
		return Value(LogicalType::MAP(LogicalTypeId::SQLNULL, LogicalTypeId::SQLNULL));
	}

	auto size = py::len(dict.keys);
	D_ASSERT(size == py::len(dict.values));

	auto key_target = MapType::KeyType(target_type);
	auto value_target = MapType::ValueType(target_type);

	LogicalType key_type = LogicalType::SQLNULL;
	LogicalType value_type = LogicalType::SQLNULL;

	vector<Value> elements;
	for (idx_t i = 0; i < size; i++) {

		Value new_key = TransformPythonValue(dict.keys.attr("__getitem__")(i), key_target);
		Value new_value = TransformPythonValue(dict.values.attr("__getitem__")(i), value_target);

		key_type = LogicalType::ForceMaxLogicalType(key_type, new_key.type());
		value_type = LogicalType::ForceMaxLogicalType(value_type, new_value.type());

		child_list_t<Value> struct_values;
		struct_values.emplace_back(make_pair("key", std::move(new_key)));
		struct_values.emplace_back(make_pair("value", std::move(new_value)));

		elements.push_back(Value::STRUCT(std::move(struct_values)));
	}
	if (key_type.id() == LogicalTypeId::SQLNULL) {
		key_type = key_target;
	}
	if (value_type.id() == LogicalTypeId::SQLNULL) {
		value_type = value_target;
	}

	LogicalType map_type = LogicalType::MAP(key_type, value_type);

	return Value::MAP(ListType::GetChildType(map_type), std::move(elements));
}

Value TransformDictionaryToMap(const PyDictionary &dict, const LogicalType &target_type = LogicalType::UNKNOWN) {
	if (target_type.id() != LogicalTypeId::UNKNOWN && !DictionaryHasMapFormat(dict)) {
		// dict == { 'k1': v1, 'k2': v2, ..., 'kn': vn }
		return TransformStructFormatDictionaryToMap(dict, target_type);
	}

	auto keys = dict.values.attr("__getitem__")(0);
	auto values = dict.values.attr("__getitem__")(1);

	if (py::none().is(keys) || py::none().is(values)) {
		// Either 'key' or 'value' is None, return early with a NULL value
		return Value(LogicalType::MAP(LogicalTypeId::SQLNULL, LogicalTypeId::SQLNULL));
	}

	auto key_size = py::len(keys);
	D_ASSERT(key_size == py::len(values));
	if (key_size == 0) {
		// dict == { 'key': [], 'value': [] }
		return EmptyMapValue();
	}

	// dict == { 'key': [ ... ], 'value' : [ ... ] }
	LogicalType key_target = LogicalTypeId::UNKNOWN;
	LogicalType value_target = LogicalTypeId::UNKNOWN;

	if (target_type.id() != LogicalTypeId::UNKNOWN) {
		key_target = LogicalType::LIST(MapType::KeyType(target_type));
		value_target = LogicalType::LIST(MapType::ValueType(target_type));
	}

	auto key_list = TransformPythonValue(keys, key_target);
	auto value_list = TransformPythonValue(values, value_target);

	LogicalType key_type = LogicalType::SQLNULL;
	LogicalType value_type = LogicalType::SQLNULL;

	vector<Value> elements;
	for (idx_t i = 0; i < key_size; i++) {

		Value new_key = ListValue::GetChildren(key_list)[i];
		Value new_value = ListValue::GetChildren(value_list)[i];

		key_type = LogicalType::ForceMaxLogicalType(key_type, new_key.type());
		value_type = LogicalType::ForceMaxLogicalType(value_type, new_value.type());

		child_list_t<Value> struct_values;
		struct_values.emplace_back(make_pair("key", std::move(new_key)));
		struct_values.emplace_back(make_pair("value", std::move(new_value)));

		elements.push_back(Value::STRUCT(std::move(struct_values)));
	}

	LogicalType map_type = LogicalType::MAP(key_type, value_type);

	return Value::MAP(ListType::GetChildType(map_type), std::move(elements));
}

Value TransformTupleToStruct(py::handle ele, const LogicalType &target_type = LogicalType::UNKNOWN) {
	auto tuple = py::cast<py::tuple>(ele);
	auto size = py::len(tuple);

	D_ASSERT(target_type.id() == LogicalTypeId::STRUCT);
	auto child_types = StructType::GetChildTypes(target_type);
	auto child_count = child_types.size();
	if (size != child_count) {
		throw InvalidInputException("Tried to create a STRUCT value from a tuple containing %d elements, but the "
		                            "STRUCT consists of %d children",
		                            size, child_count);
	}
	child_list_t<Value> children;
	for (idx_t i = 0; i < child_count; i++) {
		auto &type = child_types[i].second;
		auto &name = StructType::GetChildName(target_type, i);
		auto element = py::handle(tuple[i]);
		auto converted_value = TransformPythonValue(element, type);
		children.emplace_back(make_pair(name, std::move(converted_value)));
	}
	auto result = Value::STRUCT(std::move(children));
	return result;
}

Value TransformListValue(py::handle ele, const LogicalType &target_type = LogicalType::UNKNOWN) {
	auto size = py::len(ele);

	vector<Value> values;
	values.reserve(size);

	bool list_target = target_type.id() == LogicalTypeId::LIST;

	LogicalType element_type = LogicalType::SQLNULL;
	for (idx_t i = 0; i < size; i++) {
		auto &child_type = list_target ? ListType::GetChildType(target_type) : LogicalType::UNKNOWN;
		Value new_value = TransformPythonValue(ele.attr("__getitem__")(i), child_type);
		element_type = LogicalType::ForceMaxLogicalType(element_type, new_value.type());
		values.push_back(std::move(new_value));
	}

	return Value::LIST(element_type, values);
}

Value TransformArrayValue(py::handle ele, const LogicalType &target_type = LogicalType::UNKNOWN) {
	auto size = py::len(ele);

	vector<Value> values;
	values.reserve(size);

	bool array_target = target_type.id() == LogicalTypeId::ARRAY;
	auto &child_type = array_target ? ArrayType::GetChildType(target_type) : LogicalType::UNKNOWN;

	LogicalType element_type = LogicalType::SQLNULL;
	for (idx_t i = 0; i < size; i++) {
		Value new_value = TransformPythonValue(ele.attr("__getitem__")(i), child_type);
		element_type = LogicalType::ForceMaxLogicalType(element_type, new_value.type());
		values.push_back(std::move(new_value));
	}

	return Value::ARRAY(element_type, std::move(values));
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

PythonObjectType GetPythonObjectType(py::handle &ele) {
	auto &import_cache = *DuckDBPyConnection::ImportCache();

	if (ele.is_none()) {
		return PythonObjectType::None;
	} else if (ele.is(import_cache.pandas.NaT())) {
		return PythonObjectType::None;
	} else if (ele.is(import_cache.pandas.NA())) {
		return PythonObjectType::None;
	} else if (py::isinstance<py::bool_>(ele)) {
		return PythonObjectType::Bool;
	} else if (py::isinstance<py::int_>(ele)) {
		return PythonObjectType::Integer;
	} else if (py::isinstance<py::float_>(ele)) {
		return PythonObjectType::Float;
	} else if (py::isinstance(ele, import_cache.decimal.Decimal())) {
		return PythonObjectType::Decimal;
	} else if (py::isinstance(ele, import_cache.uuid.UUID())) {
		return PythonObjectType::Uuid;
	} else if (py::isinstance(ele, import_cache.datetime.datetime())) {
		return PythonObjectType::Datetime;
	} else if (py::isinstance(ele, import_cache.datetime.time())) {
		return PythonObjectType::Time;
	} else if (py::isinstance(ele, import_cache.datetime.date())) {
		return PythonObjectType::Date;
	} else if (py::isinstance(ele, import_cache.datetime.timedelta())) {
		return PythonObjectType::Timedelta;
	} else if (py::isinstance<py::str>(ele)) {
		return PythonObjectType::String;
	} else if (py::isinstance<py::bytearray>(ele)) {
		return PythonObjectType::ByteArray;
	} else if (py::isinstance<py::memoryview>(ele)) {
		return PythonObjectType::MemoryView;
	} else if (py::isinstance<py::bytes>(ele)) {
		return PythonObjectType::Bytes;
	} else if (py::isinstance<py::list>(ele)) {
		return PythonObjectType::List;
	} else if (py::isinstance<py::tuple>(ele)) {
		return PythonObjectType::Tuple;
	} else if (py::isinstance<py::dict>(ele)) {
		return PythonObjectType::Dict;
	} else if (ele.is(import_cache.numpy.ma.masked())) {
		return PythonObjectType::None;
	} else if (py::isinstance(ele, import_cache.numpy.ndarray())) {
		return PythonObjectType::NdArray;
	} else if (py::isinstance(ele, import_cache.numpy.datetime64())) {
		return PythonObjectType::NdDatetime;
	} else if (py::isinstance(ele, import_cache.duckdb.Value())) {
		return PythonObjectType::Value;
	} else {
		return PythonObjectType::Other;
	}
}

struct PythonValueConversion {
	static void HandleNull(Value &result, const LogicalType &target_type) {
		result = Value();
	}
	static void HandleBoolean(Value &result, const LogicalType &target_type, bool val) {
		result = Value::BOOLEAN(val);
	}
	static void HandleDouble(Value &result, const LogicalType &target_type, double val) {
		switch (target_type.id()) {
		case LogicalTypeId::UNKNOWN:
		case LogicalTypeId::DOUBLE: {
			result = Value::DOUBLE(val);
			break;
		}
		case LogicalTypeId::FLOAT: {
			result = Value::FLOAT(static_cast<float>(val));
			break;
		}
		case LogicalTypeId::DECIMAL: {
			throw ConversionException("Can't losslessly convert from object of float to type %s",
									  target_type.ToString());
		}
		default:
			throw ConversionException("Could not convert 'float' to type %s", target_type.ToString());
		}
	}
	static void HandleLargeLong(Value &result, const LogicalType &target_type, const string &val) {
		auto cast_as = target_type.id() == LogicalTypeId::UNKNOWN ? LogicalType::HUGEINT : target_type;
		result = Value(val).DefaultCastAs(cast_as);
	}
	static void HandleLongAsDouble(Value &result, const LogicalType &target_type, double val) {
		result = Value::DOUBLE(val);
	}
	static void HandleUnsignedBigint(Value &result, const LogicalType &target_type, uint64_t val) {
		result = Value::UBIGINT(val);
	}
	static void HandleBigint(Value &res, const LogicalType &target_type, int64_t value) {
		switch (target_type.id()) {
		case LogicalTypeId::UNKNOWN: {
			if (value < (int64_t)std::numeric_limits<int32_t>::min() || value > (int64_t)std::numeric_limits<int32_t>::max()) {
				res = Value::BIGINT(value);
			} else {
				// To match default duckdb behavior, numeric values without a specified type should not become a smaller type
				// than INT32
				res = Value::INTEGER(value);
			}
			break;
		}
		default:
			res = Value::BIGINT(value).DefaultCastAs(target_type);
			break;
		}
	}
	static Value HandleObjectInternal(py::handle ele, PythonObjectType object_type, const LogicalType &target_type, bool nan_as_null) {
		switch (object_type) {
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
			if (import_cache.pandas.isnull(false)) {
				auto isnull_result = import_cache.pandas.isnull()(ele);
				is_nat = string(py::str(isnull_result)) == "True";
			}
			if (is_nat) {
				return Value();
			}
			auto datetime = PyDateTime(ele);
			return datetime.ToDuckValue(target_type);
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
		case PythonObjectType::String: {
			auto stringified = ele.cast<string>();
			if (target_type.id() == LogicalTypeId::UNKNOWN) {
				return Value(stringified);
			}
			return Value(stringified).DefaultCastAs(target_type);
		}
		case PythonObjectType::ByteArray: {
			auto byte_array = ele;
			const_data_ptr_t bytes = const_data_ptr_cast(PyByteArray_AsString(byte_array.ptr())); // NOLINT
			idx_t byte_length = PyUtil::PyByteArrayGetSize(byte_array);                           // NOLINT
			return Value::BLOB(bytes, byte_length);
		}
		case PythonObjectType::MemoryView: {
			py::memoryview py_view = ele.cast<py::memoryview>();
			Py_buffer *py_buf = PyUtil::PyMemoryViewGetBuffer(py_view); // NOLINT
			return Value::BLOB(const_data_ptr_t(py_buf->buf), idx_t(py_buf->len));
		}
		case PythonObjectType::Bytes: {
			const string &ele_string = ele.cast<string>();
			switch (target_type.id()) {
			case LogicalTypeId::UNKNOWN:
			case LogicalTypeId::BLOB:
				return Value::BLOB(const_data_ptr_t(ele_string.data()), ele_string.size());
			case LogicalTypeId::BIT: {
				return Value::BIT(ele_string);
			default:
				throw ConversionException("Could not convert 'bytes' to type %s", target_type.ToString());
			}
			}
		}
		case PythonObjectType::List:
			if (target_type.id() == LogicalTypeId::ARRAY) {
				return TransformArrayValue(ele, target_type);
			} else {
				return TransformListValue(ele, target_type);
			}
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
		case PythonObjectType::Tuple: {
			switch (target_type.id()) {
			case LogicalTypeId::STRUCT:
				return TransformTupleToStruct(ele, target_type);
			case LogicalTypeId::UNKNOWN:
			case LogicalTypeId::LIST:
				return TransformListValue(ele, target_type);
			case LogicalTypeId::ARRAY:
				return TransformArrayValue(ele, target_type);
			default:
				throw InvalidInputException("Can't convert tuple to a Value of type %s", target_type.ToString());
			}
		}
		case PythonObjectType::NdArray:
		case PythonObjectType::NdDatetime:
			return TransformPythonValue(ele.attr("tolist")(), target_type, nan_as_null);
		case PythonObjectType::Value: {
			// Extract the internal object and the type from the Value instance
			auto object = ele.attr("object");
			auto type = ele.attr("type");
			shared_ptr<DuckDBPyType> internal_type;
			if (!py::try_cast<shared_ptr<DuckDBPyType>>(type, internal_type)) {
				string actual_type = py::str(type.get_type());
				throw InvalidInputException("The 'type' of a Value should be of type DuckDBPyType, not '%s'", actual_type);
			}
			return TransformPythonValue(object, internal_type->Type());
		}
		default:
			throw InternalException("Unsupported fallback");
		}
	}
	static void HandleObject(py::handle ele, PythonObjectType object_type, Value &result, const LogicalType &target_type,
						   bool nan_as_null) {
		result = HandleObjectInternal(ele, object_type, target_type, nan_as_null);
	}
};

struct PythonVectorConversion {
	static void HandleNull(Vector &result, const idx_t &result_offset) {
		FlatVector::SetNull(result, result_offset, true);
	}
	static void HandleBoolean(Vector &result, const idx_t &result_offset, bool val) {
		if (result.GetType().id() != LogicalTypeId::BOOLEAN) {
			throw TypeMismatchException(LogicalType::BOOLEAN, result.GetType(), "Python Conversion Failure: Expected a value of type %s, but got a value of type boolean");
		}
		FlatVector::GetData<bool>(result)[result_offset] = val;
	}
	static void HandleDouble(Vector &result, const idx_t &result_offset, double val) {
		switch (result.GetType().id()) {
		case LogicalTypeId::DOUBLE: {
			FlatVector::GetData<double>(result)[result_offset] = val;
			break;
		}
		case LogicalTypeId::FLOAT: {
			FlatVector::GetData<float>(result)[result_offset] = static_cast<float>(val);
			break;
		}
		default:
			throw TypeMismatchException(LogicalType::DOUBLE, result.GetType(), "Python Conversion Failure: Expected a value of type %s, but got a value of type double");
		}
	}
	static void HandleLargeLong(Vector &result, const idx_t &result_offset, const string &val) {
		FallbackValueConversion(result, result_offset, Value(val));
	}
	static void HandleLongAsDouble(Vector &result, const idx_t &result_offset, double val) {
		FallbackValueConversion(result, result_offset, Value::DOUBLE(val));
	}
	static void HandleUnsignedBigint(Vector &result, const idx_t &result_offset, uint64_t value) {
		// this code path is only called for values in the range of [INT64_MAX...UINT64_MAX]
		switch (result.GetType().id()) {
		case LogicalTypeId::HUGEINT:
			FlatVector::GetData<hugeint_t>(result)[result_offset] = Hugeint::Convert(value);
			break;
		case LogicalTypeId::UHUGEINT:
			FlatVector::GetData<uhugeint_t>(result)[result_offset] = Uhugeint::Convert(value);
			break;
		case LogicalTypeId::UBIGINT:
			FlatVector::GetData<uint64_t>(result)[result_offset] = value;
			break;
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UTINYINT:
			throw InvalidInputException("Python Conversion Failure: Value out of range for type %s", result.GetType());
		default:
			FallbackValueConversion(result, result_offset, Value::UBIGINT(value));
			break;
		}
	}
	static void HandleBigint(Vector &result, const idx_t &result_offset, int64_t value) {
		switch (result.GetType().id()) {
		case LogicalTypeId::HUGEINT: {
			FlatVector::GetData<hugeint_t>(result)[result_offset] = Hugeint::Convert(value);
			break;
		}
		case LogicalTypeId::UHUGEINT: {
			if (value < 0) {
				throw InvalidInputException("Python Conversion Failure: Value out of range for type UHUGEINT");
			}
			FlatVector::GetData<uhugeint_t>(result)[result_offset] = Uhugeint::Convert(value);
			break;
		}
		case LogicalTypeId::BIGINT: {
			FlatVector::GetData<int64_t>(result)[result_offset] = value;
			break;
		}
		case LogicalTypeId::INTEGER: {
			if (value < NumericLimits<int32_t>::Minimum() || value > NumericLimits<int32_t>::Maximum()) {
				throw InvalidInputException("Python Conversion Failure: Value out of range for type INT");
			}
			FlatVector::GetData<int32_t>(result)[result_offset] = static_cast<int32_t>(value);
			break;
		}
		case LogicalTypeId::SMALLINT: {
			if (value < NumericLimits<int16_t>::Minimum() || value > NumericLimits<int16_t>::Maximum()) {
				throw InvalidInputException("Python Conversion Failure: Value out of range for type SMALLINT");
			}
			FlatVector::GetData<int16_t>(result)[result_offset] = static_cast<int16_t>(value);
			break;
		}
		case LogicalTypeId::TINYINT: {
			if (value < NumericLimits<int8_t>::Minimum() || value > NumericLimits<int8_t>::Maximum()) {
				throw InvalidInputException("Python Conversion Failure: Value out of range for type TINYINT");
			}
			FlatVector::GetData<int8_t>(result)[result_offset] = static_cast<int8_t>(value);
			break;
		}
		case LogicalTypeId::UBIGINT: {
			if (value < 0) {
				throw InvalidInputException("Python Conversion Failure: Value out of range for type UBIGINT");
			}
			FlatVector::GetData<uint64_t>(result)[result_offset] = static_cast<uint64_t>(value);
			break;
		}
		case LogicalTypeId::UINTEGER: {
			if (value < 0 || value > (int64_t)NumericLimits<uint32_t>::Maximum()) {
				throw InvalidInputException("Python Conversion Failure: Value out of range for type UINTEGER");
			}
			FlatVector::GetData<uint32_t>(result)[result_offset] = static_cast<uint32_t>(value);
			break;
		}
		case LogicalTypeId::USMALLINT: {
			if (value < 0 || value > (int64_t)NumericLimits<uint16_t>::Maximum()) {
				throw InvalidInputException("Python Conversion Failure: Value out of range for type USMALLINT");
			}
			FlatVector::GetData<uint16_t>(result)[result_offset] = static_cast<uint16_t>(value);
			break;
		}
		case LogicalTypeId::UTINYINT: {
			if (value < 0 || value > (int64_t)NumericLimits<uint8_t>::Maximum()) {
				throw InvalidInputException("Python Conversion Failure: Value out of range for type UTINYINT");
			}
			FlatVector::GetData<uint8_t>(result)[result_offset] = static_cast<uint8_t>(value);
			break;
		}
		default:
			FallbackValueConversion(result, result_offset, Value::BIGINT(value));
			break;
		}
	}
	static void FallbackValueConversion(Vector &result, const idx_t &result_offset, Value val) {
		result.SetValue(result_offset, val);
	}
	static void HandleObject(py::handle ele, PythonObjectType object_type, Vector &result, const idx_t &result_offset,
						   bool nan_as_null) {
		Value result_val;
		PythonValueConversion::HandleObject(ele, object_type, result_val, result.GetType(), nan_as_null);
		result.SetValue(result_offset, result_val);
	}
};


template<class OP, class A, class B>
void TransformPythonObjectInternal(py::handle ele, A &result, const B &param,
						   bool nan_as_null) {
	auto object_type = GetPythonObjectType(ele);

	switch (object_type) {
	case PythonObjectType::None:
		OP::HandleNull(result, param);
		break;
	case PythonObjectType::Bool:
		OP::HandleBoolean(result, param, ele.cast<bool>());
		break;
	case PythonObjectType::Float:
		if (nan_as_null && std::isnan(PyFloat_AsDouble(ele.ptr()))) {
			OP::HandleNull(result, param);
			break;
		}
		OP::HandleDouble(result, param, ele.cast<double>());
		break;
	case PythonObjectType::Integer: {
		auto ptr = ele.ptr();

		int overflow;
		int64_t value = PyLong_AsLongLongAndOverflow(ptr, &overflow);

		if (overflow == -1) {
			PyErr_Clear();
			OP::HandleLargeLong(result, param, py::str(ele));
		} else if (overflow == 1) {
			uint64_t unsigned_value = PyLong_AsUnsignedLongLong(ptr);
			if (PyErr_Occurred()) {
				PyErr_Clear();
				double number = PyLong_AsDouble(ele.ptr());
				if (number == -1.0 && PyErr_Occurred()) {
					PyErr_Clear();
					throw InvalidInputException("An error occurred attempting to convert a python integer");
				}
				OP::HandleLongAsDouble(result, param, unsigned_value);
			} else {
				// value does not fit within an int64, but it fits within a uint64
				OP::HandleUnsignedBigint(result, param, unsigned_value);
			}
		} else if (value == -1 && PyErr_Occurred()) {
			throw InvalidInputException("An error occurred attempting to convert a python integer");
		} else {
			// value fits within an int64
			OP::HandleBigint(result, param, value);
		}
		break;
	}
	case PythonObjectType::Uuid:
	case PythonObjectType::Datetime:
	case PythonObjectType::Time:
	case PythonObjectType::Date:
	case PythonObjectType::Timedelta:
	case PythonObjectType::String:
	case PythonObjectType::ByteArray:
	case PythonObjectType::MemoryView:
	case PythonObjectType::Bytes:
	case PythonObjectType::List:
	case PythonObjectType::Dict:
	case PythonObjectType::Tuple:
	case PythonObjectType::NdArray:
	case PythonObjectType::NdDatetime:
	case PythonObjectType::Value:
	case PythonObjectType::Decimal:{
		OP::HandleObject(ele, object_type, result, param, nan_as_null);
		break;
	}
	case PythonObjectType::Other:
		throw NotImplementedException("Unable to transform python value of type '%s' to DuckDB LogicalType",
		                              py::str(ele.get_type()).cast<string>());
	default:
		throw InternalException("Object type recognized but not implemented!");
	}
}

void TransformPythonObject(py::handle ele, Vector &vector, idx_t result_offset,
						   bool nan_as_null) {
	TransformPythonObjectInternal<PythonVectorConversion>(ele, vector, result_offset, nan_as_null);
}

Value TransformPythonValue(py::handle ele, const LogicalType &target_type, bool nan_as_null) {
	Value result;
	TransformPythonObjectInternal<PythonValueConversion>(ele, result, target_type, nan_as_null);
	return result;
}

} // namespace duckdb
