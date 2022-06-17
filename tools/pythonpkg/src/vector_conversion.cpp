#include "duckdb_python/vector_conversion.hpp"
#include "duckdb_python/python_instance_checker.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

template <class T>
void ScanPandasColumn(py::array &numpy_col, idx_t stride, idx_t offset, Vector &out, idx_t count) {
	auto src_ptr = (T *)numpy_col.data();
	if (stride == sizeof(T)) {
		FlatVector::SetData(out, (data_ptr_t)(src_ptr + offset));
	} else {
		auto tgt_ptr = (T *)FlatVector::GetData(out);
		for (idx_t i = 0; i < count; i++) {
			tgt_ptr[i] = src_ptr[stride / sizeof(T) * (i + offset)];
		}
	}
}

template <class T, class V>
void ScanPandasCategoryTemplated(py::array &column, idx_t offset, Vector &out, idx_t count) {
	auto src_ptr = (T *)column.data();
	auto tgt_ptr = (V *)FlatVector::GetData(out);
	auto &tgt_mask = FlatVector::Validity(out);
	for (idx_t i = 0; i < count; i++) {
		if (src_ptr[i + offset] == -1) {
			// Null value
			tgt_mask.SetInvalid(i);
		} else {
			tgt_ptr[i] = src_ptr[i + offset];
		}
	}
}

template <class T>
void ScanPandasCategory(py::array &column, idx_t count, idx_t offset, Vector &out, string &src_type) {
	if (src_type == "int8") {
		ScanPandasCategoryTemplated<int8_t, T>(column, offset, out, count);
	} else if (src_type == "int16") {
		ScanPandasCategoryTemplated<int16_t, T>(column, offset, out, count);
	} else if (src_type == "int32") {
		ScanPandasCategoryTemplated<int32_t, T>(column, offset, out, count);
	} else {
		throw NotImplementedException("The Pandas type " + src_type + " for categorical types is not implemented yet");
	}
}

template <class T>
void ScanPandasMasked(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out) {
	ScanPandasColumn<T>(bind_data.numpy_col, bind_data.numpy_stride, offset, out, count);
	auto &result_mask = FlatVector::Validity(out);
	if (bind_data.mask) {
		auto mask = (bool *)bind_data.mask->numpy_array.data();
		for (idx_t i = 0; i < count; i++) {
			auto is_null = mask[offset + i];
			if (is_null) {
				result_mask.SetInvalid(i);
			}
		}
	}
}

template <class T>
bool ValueIsNull(T value) {
	throw std::runtime_error("unsupported type for ValueIsNull");
}

template <>
bool ValueIsNull(float value) {
	return !Value::FloatIsFinite(value);
}

template <>
bool ValueIsNull(double value) {
	return !Value::DoubleIsFinite(value);
}

template <class T>
void ScanPandasFpColumn(T *src_ptr, idx_t count, idx_t offset, Vector &out) {
	FlatVector::SetData(out, (data_ptr_t)(src_ptr + offset));
	auto tgt_ptr = FlatVector::GetData<T>(out);
	auto &mask = FlatVector::Validity(out);
	for (idx_t i = 0; i < count; i++) {
		if (ValueIsNull(tgt_ptr[i])) {
			mask.SetInvalid(i);
		}
	}
}

template <class T>
static string_t DecodePythonUnicode(T *codepoints, idx_t codepoint_count, Vector &out) {
	// first figure out how many bytes to allocate
	idx_t utf8_length = 0;
	for (idx_t i = 0; i < codepoint_count; i++) {
		int len = Utf8Proc::CodepointLength(int(codepoints[i]));
		D_ASSERT(len >= 1);
		utf8_length += len;
	}
	int sz;
	auto result = StringVector::EmptyString(out, utf8_length);
	auto target = result.GetDataWriteable();
	for (idx_t i = 0; i < codepoint_count; i++) {
		Utf8Proc::CodepointToUtf8(int(codepoints[i]), sz, target);
		D_ASSERT(sz >= 1);
		target += sz;
	}
	result.Finalize();
	return result;
}

template <typename T>
bool	TryCast(const py::object stuf, T& value){
    try{
        value = stuf.cast<T>();
        return true;
    }catch(py::cast_error){
		return false;
    }
}

template<typename T>
T	Cast(const py::object obj) {
	return obj.cast<T>();
}

void ScanPandasObject(PandasColumnBindData& bind_data, py::handle object, idx_t count, idx_t offset, Vector& out) {
	auto& type = out.GetType();
	switch (type.id()) {
		case LogicalTypeId::INTEGER: {
			if (py::isinstance<py::int_>(object)) {
				auto val = py::cast<int32_t>(object);
			}
		}
	}
}

void VectorConversion::NumpyToDuckDB(PandasColumnBindData &bind_data, py::array &numpy_col, idx_t count, idx_t offset,
                                     Vector &out) {
	switch (bind_data.pandas_type) {
	case PandasType::BOOLEAN:
		ScanPandasMasked<bool>(bind_data, count, offset, out);
		break;
	case PandasType::BOOL:
		ScanPandasColumn<bool>(numpy_col, bind_data.numpy_stride, offset, out, count);
		break;
	case PandasType::UTINYINT:
		ScanPandasMasked<uint8_t>(bind_data, count, offset, out);
		break;
	case PandasType::USMALLINT:
		ScanPandasMasked<uint16_t>(bind_data, count, offset, out);
		break;
	case PandasType::UINTEGER:
		ScanPandasMasked<uint32_t>(bind_data, count, offset, out);
		break;
	case PandasType::UBIGINT:
		ScanPandasMasked<uint64_t>(bind_data, count, offset, out);
		break;
	case PandasType::TINYINT:
		ScanPandasMasked<int8_t>(bind_data, count, offset, out);
		break;
	case PandasType::SMALLINT:
		ScanPandasMasked<int16_t>(bind_data, count, offset, out);
		break;
	case PandasType::INTEGER:
		ScanPandasMasked<int32_t>(bind_data, count, offset, out);
		break;
	case PandasType::BIGINT:
		ScanPandasMasked<int64_t>(bind_data, count, offset, out);
		break;
	case PandasType::FLOAT:
		ScanPandasFpColumn<float>((float *)numpy_col.data(), count, offset, out);
		break;
	case PandasType::DOUBLE:
		ScanPandasFpColumn<double>((double *)numpy_col.data(), count, offset, out);
		break;
	case PandasType::TIMESTAMP: {
		auto src_ptr = (int64_t *)numpy_col.data();
		auto tgt_ptr = FlatVector::GetData<timestamp_t>(out);
		auto &mask = FlatVector::Validity(out);

		for (idx_t row = 0; row < count; row++) {
			auto source_idx = offset + row;
			if (src_ptr[source_idx] <= NumericLimits<int64_t>::Minimum()) {
				// pandas Not a Time (NaT)
				mask.SetInvalid(row);
				continue;
			}
			tgt_ptr[row] = Timestamp::FromEpochNanoSeconds(src_ptr[source_idx]);
		}
		break;
	}
	case PandasType::INTERVAL: {
		auto src_ptr = (int64_t *)numpy_col.data();
		auto tgt_ptr = FlatVector::GetData<interval_t>(out);
		auto &mask = FlatVector::Validity(out);

		for (idx_t row = 0; row < count; row++) {
			auto source_idx = offset + row;
			if (src_ptr[source_idx] <= NumericLimits<int64_t>::Minimum()) {
				// pandas Not a Time (NaT)
				mask.SetInvalid(row);
				continue;
			}
			int64_t micro = src_ptr[source_idx] / 1000;
			int64_t days = micro / Interval::MICROS_PER_DAY;
			micro = micro % Interval::MICROS_PER_DAY;
			int64_t months = days / Interval::DAYS_PER_MONTH;
			days = days % Interval::DAYS_PER_MONTH;
			interval_t interval;
			interval.months = months;
			interval.days = days;
			interval.micros = micro;
			tgt_ptr[row] = interval;
		}
		break;
	}
	case PandasType::VARCHAR:
	case PandasType::OBJECT: {
		// Get the source pointer of the numpy array
		auto src_ptr = (PyObject **)numpy_col.data();

		// Get the data pointer and the validity mask of the result vector
		auto tgt_ptr = FlatVector::GetData<string_t>(out);
		auto &out_mask = FlatVector::Validity(out);
		unique_ptr<PythonGILWrapper> gil;

		// Loop over every row of the arrays contents
		for (idx_t row = 0; row < count; row++) {
			auto source_idx = offset + row;

			// Get the pointer to the object
			PyObject *val = src_ptr[source_idx];
			if (bind_data.pandas_type == PandasType::OBJECT && !PyUnicode_CheckExact(val)) {
				LogicalType ltype = out.GetType();
				if (val == Py_None) {
					out_mask.SetInvalid(row);
					continue;
				}
				if (py::isinstance<py::float_>(val) && std::isnan(PyFloat_AsDouble(val))) {
					out_mask.SetInvalid(row);
					continue;
				}
				if (!py::isinstance<py::str>(val)) {
					if (!gil) {
						gil = bind_data.object_str_val.GetLock();
					}
					bind_data.object_str_val.AssignInternal<PyObject>(
					    [](py::str &obj, PyObject &new_val) {
						    py::handle object_handle = &new_val;
						    obj = py::str(object_handle);
					    },
					    *val, *gil);
					val = (PyObject *)bind_data.object_str_val.GetPointerTop()->ptr();
				}
			}
			// Python 3 string representation:
			// https://github.com/python/cpython/blob/3a8fdb28794b2f19f6c8464378fb8b46bce1f5f4/Include/cpython/unicodeobject.h#L79
			if (!PyUnicode_CheckExact(val)) {
				out_mask.SetInvalid(row);
				continue;
			}
			if (PyUnicode_IS_COMPACT_ASCII(val)) {
				// ascii string: we can zero copy
				tgt_ptr[row] = string_t((const char *)PyUnicode_DATA(val), PyUnicode_GET_LENGTH(val));
			} else {
				// unicode gunk
				auto ascii_obj = (PyASCIIObject *)val;
				auto unicode_obj = (PyCompactUnicodeObject *)val;
				// compact unicode string: is there utf8 data available?
				if (unicode_obj->utf8) {
					// there is! zero copy
					tgt_ptr[row] = string_t((const char *)unicode_obj->utf8, unicode_obj->utf8_length);
				} else if (PyUnicode_IS_COMPACT(unicode_obj) && !PyUnicode_IS_ASCII(unicode_obj)) {
					auto kind = PyUnicode_KIND(val);
					switch (kind) {
					case PyUnicode_1BYTE_KIND:
						tgt_ptr[row] =
						    DecodePythonUnicode<Py_UCS1>(PyUnicode_1BYTE_DATA(val), PyUnicode_GET_LENGTH(val), out);
						break;
					case PyUnicode_2BYTE_KIND:
						tgt_ptr[row] =
						    DecodePythonUnicode<Py_UCS2>(PyUnicode_2BYTE_DATA(val), PyUnicode_GET_LENGTH(val), out);
						break;
					case PyUnicode_4BYTE_KIND:
						tgt_ptr[row] =
						    DecodePythonUnicode<Py_UCS4>(PyUnicode_4BYTE_DATA(val), PyUnicode_GET_LENGTH(val), out);
						break;
					default:
						throw std::runtime_error("Unsupported typekind for Python Unicode Compact decode");
					}
				} else if (ascii_obj->state.kind == PyUnicode_WCHAR_KIND) {
					throw std::runtime_error("Unsupported: decode not ready legacy string");
				} else if (!PyUnicode_IS_COMPACT(unicode_obj) && ascii_obj->state.kind != PyUnicode_WCHAR_KIND) {
					throw std::runtime_error("Unsupported: decode ready legacy string");
				} else {
					throw std::runtime_error("Unsupported string type: no clue what this string is");
				}
			}
		}
		break;
	}
	case PandasType::CATEGORY: {
		switch (out.GetType().InternalType()) {
		case PhysicalType::UINT8:
			ScanPandasCategory<uint8_t>(numpy_col, count, offset, out, bind_data.internal_categorical_type);
			break;
		case PhysicalType::UINT16:
			ScanPandasCategory<uint16_t>(numpy_col, count, offset, out, bind_data.internal_categorical_type);
			break;
		case PhysicalType::UINT32:
			ScanPandasCategory<uint32_t>(numpy_col, count, offset, out, bind_data.internal_categorical_type);
			break;
		default:
			throw InternalException("Invalid Physical Type for ENUMs");
		}
		break;
	}

	default:
		throw std::runtime_error("Unsupported type " + out.GetType().ToString());
	}
}

static py::object GetItem(pybind11::object &column, idx_t index) {
	return column.attr("__getitem__")(index);
}

static duckdb::LogicalType GetItemType(PythonInstanceChecker& instance_checker, py::handle &ele, bool &can_convert);

static duckdb::LogicalType GetListType(PythonInstanceChecker& instance_checker, py::handle &ele, bool &can_convert) {
	auto size = py::len(ele);

	if (size == 0) {
		return LogicalType::LIST(LogicalType::SQLNULL);
	}

	vector<LogicalType> child_types;
	child_types.reserve(size);

	idx_t i = 0;
	LogicalType list_type = LogicalType::SQLNULL;
	for (auto py_val : ele) {
		auto item_type = GetItemType(instance_checker, py_val, can_convert);
		if (!i) {
			list_type = item_type;
		} else {
			if (item_type != list_type) {
				can_convert = false;
			}
		}
		if (!can_convert) {
			break;
		}
		i++;
	}
	return LogicalType::LIST(list_type);
}

static duckdb::LogicalType EmptyMap() {
	child_list_t<LogicalType> child_types;
	auto empty = LogicalType::LIST(LogicalTypeId::SQLNULL);
	child_types.push_back(make_pair("key", empty));
	child_types.push_back(make_pair("value", empty));
	return LogicalType::MAP(move(child_types));
}

//! 'can_convert' is used to communicate if internal structures encountered here are valid
//! for example a python list could contain of multiple different types, which we cant communicate downwards through
//! LogicalType's alone
static duckdb::LogicalType GetItemType(PythonInstanceChecker& instance_checker, py::handle &ele, bool &can_convert) {
	auto datetime_mod = py::module_::import("datetime");
	auto decimal_mod = py::module_::import("decimal");

	auto datetime_date = datetime_mod.attr("date");
	auto datetime_datetime = datetime_mod.attr("datetime");
	auto datetime_time = datetime_mod.attr("time");

	auto decimal_decimal = decimal_mod.attr("Decimal");

	if (ele.is_none()) {
		return LogicalType::SQLNULL;
	} else if (instance_checker.IsInstanceOf<py::bool_>(ele)) {
		return LogicalType::BOOLEAN;
	} else if (instance_checker.IsInstanceOf<py::int_>(ele)) {
		return LogicalType::BIGINT;
	} else if (instance_checker.IsInstanceOf<py::float_>(ele)) {
		return LogicalType::DOUBLE;
	} else if (instance_checker.IsInstanceOf(ele, "Decimal", "decimal")) {
		return LogicalType::VARCHAR; // Might be float64 actually?
	} else if (instance_checker.IsInstanceOf(ele, "datetime", "datetime")) {
		// auto ptr = ele.ptr();
		// auto year = PyDateTime_GET_YEAR(ptr);
		// auto month = PyDateTime_GET_MONTH(ptr);
		// auto day = PyDateTime_GET_DAY(ptr);
		// auto hour = PyDateTime_DATE_GET_HOUR(ptr);
		// auto minute = PyDateTime_DATE_GET_MINUTE(ptr);
		// auto second = PyDateTime_DATE_GET_SECOND(ptr);
		// auto micros = PyDateTime_DATE_GET_MICROSECOND(ptr);
		// This probably needs to be more precise ..
		return LogicalType::TIMESTAMP;
	} else if (instance_checker.IsInstanceOf(ele, "time", "datetime")) {
		// auto ptr = ele.ptr();
		// auto hour = PyDateTime_TIME_GET_HOUR(ptr);
		// auto minute = PyDateTime_TIME_GET_MINUTE(ptr);
		// auto second = PyDateTime_TIME_GET_SECOND(ptr);
		// auto micros = PyDateTime_TIME_GET_MICROSECOND(ptr);
		return LogicalType::TIME;
	} else if (instance_checker.IsInstanceOf(ele, "date", "datetime")) {
		// auto ptr = ele.ptr();
		// auto year = PyDateTime_GET_YEAR(ptr);
		// auto month = PyDateTime_GET_MONTH(ptr);
		// auto day = PyDateTime_GET_DAY(ptr);
		return LogicalType::DATE;
	} else if (instance_checker.IsInstanceOf<py::str>(ele)) {
		return LogicalType::VARCHAR;
	} else if (instance_checker.IsInstanceOf<py::memoryview>(ele)) {
		return LogicalType::BLOB;
	} else if (instance_checker.IsInstanceOf<py::bytes>(ele)) {
		return LogicalType::BLOB;
	} else if (instance_checker.IsInstanceOf<py::list>(ele)) {
		return GetListType(instance_checker, ele, can_convert);
	} else if (instance_checker.IsInstanceOf<py::dict>(ele)) {
		auto keys = ele.attr("keys")();
		auto values = ele.attr("values")();
		auto size = py::len(keys);

		if (size == 0) {
			return EmptyMap();
		}
		child_list_t<LogicalType> child_types;
		auto key_type = GetListType(instance_checker, keys, can_convert);
		if (!can_convert) {
			return EmptyMap();
		}
		auto value_type = GetListType(instance_checker, values, can_convert);
		if (!can_convert) {
			return EmptyMap();
		}

		child_types.push_back(make_pair("key", key_type));
		child_types.push_back(make_pair("value", value_type));
		return LogicalType::MAP(move(child_types));
	} else {
		throw std::runtime_error("unknown param type " + py::str(ele.get_type()).cast<string>());
	}
}

// PandasType ConvertLogicalTypeToPandas(const LogicalType& type) {
//	switch (type.id()) {
//		case LogicalTypeId::TINYINT: return PandasType::TINYINT;
//		case LogicalTypeId::SMALLINT: return PandasType::SMALLINT;
//		case LogicalTypeId::INTEGER: return PandasType::INTEGER;
//		case LogicalTypeId::BIGINT: return PandasType::BIGINT;
//		case LogicalTypeId::HUGEINT: return PandasType::OBJECT;
//		case LogicalTypeId::CHAR:
//		case LogicalTypeId::UTINYINT: return PandasType::UTINYINT;
//		case LogicalTypeId::USMALLINT: return PandasType::USMALLINT;
//		case LogicalTypeId::UINTEGER: return PandasType::UINTEGER;
//		case LogicalTypeId::UBIGINT: return PandasType::UBIGINT;
//		case LogicalTypeId::VARCHAR: return PandasType::VARCHAR;
//		case LogicalTypeId::DECIMAL:
//		case LogicalTypeId::FLOAT: return PandasType::FLOAT;
//		case LogicalTypeId::DOUBLE: return PandasType::DOUBLE;
//		case LogicalTypeId::BOOLEAN: return PandasType::BOOLEAN;
//		//timedelta?
//		case LogicalTypeId::TIME:
//		case LogicalTypeId::TIME_TZ:
//		//datetime64
//		case LogicalTypeId::DATE:
//		case LogicalTypeId::TIMESTAMP:
//		case LogicalTypeId::TIMESTAMP_MS:
//		case LogicalTypeId::TIMESTAMP_NS:
//		case LogicalTypeId::TIMESTAMP_SEC:
//		case LogicalTypeId::TIMESTAMP_TZ: return PandasType::TIMESTAMP;
//		case LogicalTypeId::USER: return PandasType::CATEGORY;
//		default: return PandasType::OBJECT;
//	}
// }

static duckdb::LogicalType AnalyzeObjectType(PythonInstanceChecker& instance_checker, pybind11::object column, bool &can_convert) {
	idx_t rows = py::len(column);
	LogicalType item_type = duckdb::LogicalType::SQLNULL;
	if (!rows) {
		return item_type;
	}

	auto first_item = GetItem(column, 0);
	item_type = GetItemType(instance_checker, first_item, can_convert);
	if (!can_convert) {
		return item_type;
	}

	for (idx_t i = 1; i < rows; i++) {
		auto next_item = GetItem(column, i);
		auto next_item_type = GetItemType(instance_checker, next_item, can_convert);
		if (!can_convert) {
			break;
		}
		item_type = LogicalType::MaxLogicalType(item_type, next_item_type);
	}

	return item_type;
}

static void ConvertPandasType(ExtendedNumpyType &col_type, LogicalType &duckdb_col_type, PandasType &pandas_type) {
	switch (col_type) {
	case ExtendedNumpyType::BOOL: {
		duckdb_col_type = LogicalType::BOOLEAN;
		pandas_type = PandasType::BOOL;
		break;
	}
	case ExtendedNumpyType::PANDA_BOOL: {
		duckdb_col_type = LogicalType::BOOLEAN;
		pandas_type = PandasType::BOOLEAN;
		break;
	}
	case ExtendedNumpyType::PANDA_INT8:
	case ExtendedNumpyType::INT_8: {
		duckdb_col_type = LogicalType::TINYINT;
		pandas_type = PandasType::TINYINT;
		break;
	}
	case ExtendedNumpyType::PANDA_UINT8:
	case ExtendedNumpyType::UINT_8: {
		duckdb_col_type = LogicalType::UTINYINT;
		pandas_type = PandasType::UTINYINT;
		break;
	}
	case ExtendedNumpyType::PANDA_INT16:
	case ExtendedNumpyType::INT_16: {
		duckdb_col_type = LogicalType::SMALLINT;
		pandas_type = PandasType::SMALLINT;
		break;
	}
	case ExtendedNumpyType::PANDA_UINT16:
	case ExtendedNumpyType::UINT_16: {
		duckdb_col_type = LogicalType::USMALLINT;
		pandas_type = PandasType::USMALLINT;
		break;
	}
	case ExtendedNumpyType::PANDA_INT32:
	case ExtendedNumpyType::INT_32: {
		duckdb_col_type = LogicalType::INTEGER;
		pandas_type = PandasType::INTEGER;
		break;
	}
	case ExtendedNumpyType::PANDA_UINT32:
	case ExtendedNumpyType::UINT_32: {
		duckdb_col_type = LogicalType::UINTEGER;
		pandas_type = PandasType::UINTEGER;
		break;
	}
	case ExtendedNumpyType::PANDA_INT64:
	case ExtendedNumpyType::INT_64: {
		duckdb_col_type = LogicalType::BIGINT;
		pandas_type = PandasType::BIGINT;
		break;
	}
	case ExtendedNumpyType::PANDA_UINT64:
	case ExtendedNumpyType::UINT_64: {
		duckdb_col_type = LogicalType::UBIGINT;
		pandas_type = PandasType::UBIGINT;
		break;
	}
	case ExtendedNumpyType::PANDA_FLOAT32:
	case ExtendedNumpyType::FLOAT_32: {
		duckdb_col_type = LogicalType::FLOAT;
		pandas_type = PandasType::FLOAT;
		break;
	}
	case ExtendedNumpyType::PANDA_FLOAT64:
	case ExtendedNumpyType::LONG_FLOAT_64:
	case ExtendedNumpyType::FLOAT_64: {
		duckdb_col_type = LogicalType::DOUBLE;
		pandas_type = PandasType::DOUBLE;
		break;
	}
	case ExtendedNumpyType::OBJECT: {
		duckdb_col_type = LogicalType::VARCHAR;
		pandas_type = PandasType::OBJECT;
		break;
	}
	case ExtendedNumpyType::PANDA_STRING: {
		duckdb_col_type = LogicalType::VARCHAR;
		pandas_type = PandasType::VARCHAR;
		break;
	}
	case ExtendedNumpyType::TIMEDELTA: {
		duckdb_col_type = LogicalType::INTERVAL;
		pandas_type = PandasType::INTERVAL;
		break;
	}
	case ExtendedNumpyType::PANDA_DATETIME:
	case ExtendedNumpyType::DATETIME: {
		duckdb_col_type = LogicalType::TIMESTAMP;
		pandas_type = PandasType::TIMESTAMP;
		break;
	}
	default: {
		throw std::runtime_error("Failed to convert dtype num " + to_string((uint8_t)col_type) +
		                         " to duckdb LogicalType");
	}
	}
}

template <class T>
ExtendedNumpyType GetExtendedNumpyType(T dtype) {
	int64_t extended_type;

	if (py::hasattr(dtype, "num")) {
		extended_type = py::int_(dtype.attr("num"));
	} else {
		auto type_str = string(py::str(dtype));
		if (type_str == "boolean") {
			return ExtendedNumpyType::PANDA_BOOL;
		} else if (type_str == "category") {
			return ExtendedNumpyType::PANDA_CATEGORY;
		} else if (type_str == "Int8") {
			return ExtendedNumpyType::PANDA_INT8;
		} else if (type_str == "Int16") {
			return ExtendedNumpyType::PANDA_INT16;
		} else if (type_str == "Int32") {
			return ExtendedNumpyType::PANDA_INT32;
		} else if (type_str == "Int64") {
			return ExtendedNumpyType::PANDA_INT64;
		} else if (type_str == "UInt8") {
			return ExtendedNumpyType::PANDA_UINT8;
		} else if (type_str == "UInt16") {
			return ExtendedNumpyType::PANDA_UINT16;
		} else if (type_str == "UInt32") {
			return ExtendedNumpyType::PANDA_UINT32;
		} else if (type_str == "UInt64") {
			return ExtendedNumpyType::PANDA_UINT64;
		} else if (type_str == "Float32") {
			return ExtendedNumpyType::PANDA_FLOAT32;
		} else if (type_str == "Float64") {
			return ExtendedNumpyType::PANDA_FLOAT64;
		} else if (type_str == "string") {
			return ExtendedNumpyType::PANDA_STRING;
		} else {
			throw std::runtime_error("Unknown dtype (" + type_str + ")");
		}
	}
	// 100 (PANDA_EXTENSION_TYPE) is potentially used by multiple dtypes, need to figure out which one it is exactly.
	if (extended_type == (int64_t)ExtendedNumpyType::PANDA_EXTENSION_TYPE) {
		auto extension_type_str = string(py::str(dtype));
		if (extension_type_str == "category") {
			return ExtendedNumpyType::PANDA_CATEGORY;
		} else {
			throw std::runtime_error("Unknown extension dtype (" + extension_type_str + ")");
		}
	}
	// Since people can extend the dtypes with their own custom stuff, it's probably best to check if it falls out of
	// the supported range of dtypes. (little hardcoded though)
	if (!(extended_type >= 0 && extended_type <= 23) && !(extended_type >= 100 && extended_type <= 103)) {
		throw std::runtime_error("Dtype num " + to_string(extended_type) + " is not supported");
	}
	return (ExtendedNumpyType)extended_type;
}

bool ColumnIsMasked(pybind11::detail::accessor<pybind11::detail::accessor_policies::list_item> column,
                    const ExtendedNumpyType &type) {
	bool masked = py::hasattr(column, "mask");
	return (masked || type == ExtendedNumpyType::PANDA_INT8 || type == ExtendedNumpyType::PANDA_INT16 ||
	        type == ExtendedNumpyType::PANDA_INT32 || type == ExtendedNumpyType::PANDA_INT64 ||
	        type == ExtendedNumpyType::PANDA_UINT8 || type == ExtendedNumpyType::PANDA_UINT16 ||
	        type == ExtendedNumpyType::PANDA_UINT32 || type == ExtendedNumpyType::PANDA_UINT64 ||
	        type == ExtendedNumpyType::PANDA_FLOAT32 || type == ExtendedNumpyType::PANDA_FLOAT64 ||
	        type == ExtendedNumpyType::PANDA_BOOL);
}

void VectorConversion::BindPandas(py::handle df, vector<PandasColumnBindData> &bind_columns,
                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto instance_checker = make_shared<PythonInstanceChecker>();
	// This performs a shallow copy that allows us to rename the dataframe
	auto df_columns = py::list(df.attr("columns"));
	auto df_types = py::list(df.attr("dtypes"));
	auto get_fun = df.attr("__getitem__");
	// TODO support masked arrays as well
	// TODO support dicts of numpy arrays as well
	if (py::len(df_columns) == 0 || py::len(df_types) == 0 || py::len(df_columns) != py::len(df_types)) {
		throw std::runtime_error("Need a DataFrame with at least one column");
	}
	py::array column_attributes = df.attr("columns").attr("values");

	// loop over every column
	for (idx_t col_idx = 0; col_idx < py::len(df_columns); col_idx++) {
		LogicalType duckdb_col_type;
		PandasColumnBindData bind_data;

		names.emplace_back(py::str(df_columns[col_idx]));
		auto dtype = GetExtendedNumpyType(df_types[col_idx]);
		bool masked = ColumnIsMasked(df_columns[col_idx], dtype);
		// auto col_type = string(py::str(df_types[col_idx]));

		bind_data.mask = nullptr;
		if (masked) {
			// masked object
			// fetch the internal data and mask array
			bind_data.mask = make_unique<NumPyArrayWrapper>(get_fun(df_columns[col_idx]).attr("array").attr("_mask"));
		}

		auto column = get_fun(df_columns[col_idx]);
		if (dtype == ExtendedNumpyType::PANDA_CATEGORY) {
			// for category types, we create an ENUM type for string or use the converted numpy type for the rest
			D_ASSERT(py::hasattr(column, "cat"));
			D_ASSERT(py::hasattr(column.attr("cat"), "categories"));
			auto categories = py::array(column.attr("cat").attr("categories"));

			auto category_type = GetExtendedNumpyType(categories.attr("dtype"));
			if (category_type == ExtendedNumpyType::OBJECT) {
				// Let's hope the object type is a string.
				bind_data.pandas_type = PandasType::CATEGORY;
				auto enum_name = string(py::str(df_columns[col_idx]));
				vector<string> enum_entries = py::cast<vector<string>>(categories);
				idx_t size = enum_entries.size();
				Vector enum_entries_vec(LogicalType::VARCHAR, size);
				auto enum_entries_ptr = FlatVector::GetData<string_t>(enum_entries_vec);
				for (idx_t i = 0; i < size; i++) {
					enum_entries_ptr[i] = StringVector::AddStringOrBlob(enum_entries_vec, enum_entries[i]);
				}
				D_ASSERT(py::hasattr(column.attr("cat"), "codes"));
				duckdb_col_type = LogicalType::ENUM(enum_name, enum_entries_vec, size);
				bind_data.numpy_col = py::array(column.attr("cat").attr("codes"));
				D_ASSERT(py::hasattr(bind_data.numpy_col, "dtype"));
				bind_data.internal_categorical_type = string(py::str(bind_data.numpy_col.attr("dtype")));
			} else {
				bind_data.numpy_col = py::array(column.attr("to_numpy")());
				auto numpy_type = bind_data.numpy_col.attr("dtype");
				// for category types (non-strings), we use the converted numpy type
				category_type = GetExtendedNumpyType(numpy_type);
				ConvertPandasType(category_type, duckdb_col_type, bind_data.pandas_type);
			}
		} else {
			if (masked || dtype == ExtendedNumpyType::DATETIME || dtype == ExtendedNumpyType::PANDA_DATETIME) {
				bind_data.numpy_col = get_fun(df_columns[col_idx]).attr("array").attr("_data");
			} else {
				bind_data.numpy_col = py::array(column.attr("to_numpy")());
			}
			ConvertPandasType(dtype, duckdb_col_type, bind_data.pandas_type);
		}

		// Analyze the inner data type of the 'object' column
		if (bind_data.pandas_type == PandasType::OBJECT) {
			bool can_convert = true;
			LogicalType converted_type = AnalyzeObjectType(*instance_checker, get_fun(df_columns[col_idx]), can_convert);
			if (can_convert) {
				duckdb_col_type = converted_type;
			}
		}

		D_ASSERT(py::hasattr(bind_data.numpy_col, "strides"));
		bind_data.numpy_stride = bind_data.numpy_col.attr("strides").attr("__getitem__")(0).cast<idx_t>();
		return_types.push_back(duckdb_col_type);
		bind_columns.push_back(move(bind_data));
	}
}

//'?'
//
// boolean
//
//'b'
//
//(signed) byte
//
//'B'
//
// unsigned byte
//
//'i'
//
//(signed) integer
//
//'u'
//
// unsigned integer
//
//'f'
//
// floating-point
//
//'c'
//
// complex-floating point
//
//'m'
//
// timedelta
//
//'M'
//
// datetime
//
//'O'
//
//(Python) objects
//
//'S', 'a'
//
// zero-terminated bytes (not recommended)
//
//'U'
//
// Unicode string
//
//'V'
//
// raw data (void)
// test

static string GetItemDataType(idx_t index, pybind11::object &col) {
	return py::str(col.attr("__getitem__")(index).get_type().attr("__name__"));
}

static bool MeetsConversionRequirements(const string &type, pybind11::object &column, idx_t rows) {

	if (type == "object") {
		// Can't change
		return false;
	}
	for (size_t i = 1; i < rows; i++) {
		string other = GetItemDataType(i, column);
		if (type != other) {
			// Not all the same type
			return false;
		}
	}
	return true;
}

static void ConvertSingleColumn(py::handle df, idx_t col_idx) {
	auto df_columns = py::list(df.attr("columns"));
	auto df_types = py::list(df.attr("dtypes"));
	auto get_fun = df.attr("__getitem__");
	auto set_fun = df.attr("__setitem__");

	// We gotta check if all the types of the column are the same, if yes we can change it
	auto column = get_fun(df_columns[col_idx]);

	idx_t rows = py::len(column);
	D_ASSERT(rows > 0);
	string column_type = GetItemDataType(0, column);
	if (!MeetsConversionRequirements(column_type, column, rows)) {
		return;
	}

	if (column_type == "date") {
		set_fun(df_columns[col_idx], column.attr("astype")("M", py::arg("copy") = false));
	}
	// pandas uses native python strings, which have to stay 'object'
	else if (column_type == "str") {
		return;
	} else if (column_type == "int") {
		set_fun(df_columns[col_idx], column.attr("astype")("i", py::arg("copy") = false));
	} else if (column_type == "bool") {
		set_fun(df_columns[col_idx], column.attr("astype")("?", py::arg("copy") = false));
	} else if (column_type == "float") {
		set_fun(df_columns[col_idx], column.attr("astype")("f", py::arg("copy") = false));
	} else if (column_type == "clongdouble") {
		set_fun(df_columns[col_idx], column.attr("astype")("G", py::arg("copy") = false));
	} else if (column_type == "longdouble") {
		set_fun(df_columns[col_idx], column.attr("astype")("g", py::arg("copy") = false));
	} else if (column_type == "complex") {
		set_fun(df_columns[col_idx], column.attr("astype")("D", py::arg("copy") = false));
	} else if (column_type == "bytes") {
		// These are essentially just fixed-size strings, so it's fine if they stay object
		return;
	} else if (column_type == "timedelta") {
		set_fun(df_columns[col_idx], column.attr("astype")("m", py::arg("copy") = false));
	} else {
		throw std::runtime_error(column_type);
	}
}

void VectorConversion::Analyze(py::handle df) {
	auto df_columns = py::list(df.attr("columns"));
	auto df_types = py::list(df.attr("dtypes"));
	auto empty = py::bool_(df.attr("empty"));
	auto get_fun = df.attr("__getitem__");
	auto set_fun = df.attr("__setitem__");

	if (empty) {
		return;
	}

	if (py::len(df_columns) == 0 || py::len(df_types) == 0 || py::len(df_columns) != py::len(df_types)) {
		throw std::runtime_error("Need a DataFrame with at least one column");
	}
	for (idx_t col_idx = 0; col_idx < py::len(df_columns); col_idx++) {
		auto col_type = string(py::str(df_types[col_idx]));
		if (col_type != "object") {
			continue;
		}
		ConvertSingleColumn(df, col_idx);
	}
}
} // namespace duckdb
