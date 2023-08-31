#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/python_objects.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

namespace duckdb_py_convert {

struct RegularConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static NUMPY_T ConvertValue(DUCKDB_T val) {
		return (NUMPY_T)val;
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct TimestampConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(timestamp_t val) {
		return Timestamp::GetEpochNanoSeconds(val);
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct TimestampConvertSec {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(timestamp_t val) {
		return Timestamp::GetEpochNanoSeconds(Timestamp::FromEpochSeconds(val.value));
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct TimestampConvertMilli {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(timestamp_t val) {
		return Timestamp::GetEpochNanoSeconds(Timestamp::FromEpochMs(val.value));
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct TimestampConvertNano {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(timestamp_t val) {
		return val.value;
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct DateConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(date_t val) {
		return Date::EpochNanoseconds(val);
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct IntervalConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(interval_t val) {
		return Interval::GetNanoseconds(val);
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct TimeConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(dtime_t val) {
		auto str = duckdb::Time::ToString(val);
		return PyUnicode_FromStringAndSize(str.c_str(), str.size());
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return nullptr;
	}
};

struct StringConvert {
	template <class T>
	static void ConvertUnicodeValueTemplated(T *result, int32_t *codepoints, idx_t codepoint_count, const char *data,
	                                         idx_t ascii_count) {
		// we first fill in the batch of ascii characters directly
		for (idx_t i = 0; i < ascii_count; i++) {
			result[i] = data[i];
		}
		// then we fill in the remaining codepoints from our codepoint array
		for (idx_t i = 0; i < codepoint_count; i++) {
			result[ascii_count + i] = codepoints[i];
		}
	}

	static PyObject *ConvertUnicodeValue(const char *data, idx_t len, idx_t start_pos) {
		// slow path: check the code points
		// we know that all characters before "start_pos" were ascii characters, so we don't need to check those

		// allocate an array of code points so we only have to convert the codepoints once
		// short-string optimization
		// we know that the max amount of codepoints is the length of the string
		// for short strings (less than 64 bytes) we simply statically allocate an array of 256 bytes (64x int32)
		// this avoids memory allocation for small strings (common case)
		idx_t remaining = len - start_pos;
		unique_ptr<int32_t[]> allocated_codepoints;
		int32_t static_codepoints[64];
		int32_t *codepoints;
		if (remaining > 64) {
			allocated_codepoints = unique_ptr<int32_t[]>(new int32_t[remaining]);
			codepoints = allocated_codepoints.get();
		} else {
			codepoints = static_codepoints;
		}
		// now we iterate over the remainder of the string to convert the UTF8 string into a sequence of codepoints
		// and to find the maximum codepoint
		int32_t max_codepoint = 127;
		int sz;
		idx_t pos = start_pos;
		idx_t codepoint_count = 0;
		while (pos < len) {
			codepoints[codepoint_count] = Utf8Proc::UTF8ToCodepoint(data + pos, sz);
			pos += sz;
			if (codepoints[codepoint_count] > max_codepoint) {
				max_codepoint = codepoints[codepoint_count];
			}
			codepoint_count++;
		}
		// based on the max codepoint, we construct the result string
		auto result = PyUnicode_New(start_pos + codepoint_count, max_codepoint);
		// based on the resulting unicode kind, we fill in the code points
		auto result_handle = py::handle(result);
		auto kind = PyUtil::PyUnicodeKind(result_handle);
		switch (kind) {
		case PyUnicode_1BYTE_KIND:
			ConvertUnicodeValueTemplated<Py_UCS1>(PyUtil::PyUnicode1ByteData(result_handle), codepoints,
			                                      codepoint_count, data, start_pos);
			break;
		case PyUnicode_2BYTE_KIND:
			ConvertUnicodeValueTemplated<Py_UCS2>(PyUtil::PyUnicode2ByteData(result_handle), codepoints,
			                                      codepoint_count, data, start_pos);
			break;
		case PyUnicode_4BYTE_KIND:
			ConvertUnicodeValueTemplated<Py_UCS4>(PyUtil::PyUnicode4ByteData(result_handle), codepoints,
			                                      codepoint_count, data, start_pos);
			break;
		default:
			throw NotImplementedException("Unsupported typekind constant '%d' for Python Unicode Compact decode", kind);
		}
		return result;
	}

	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(string_t val) {
		// we could use PyUnicode_FromStringAndSize here, but it does a lot of verification that we don't need
		// because of that it is a lot slower than it needs to be
		auto data = const_data_ptr_cast(val.GetData());
		auto len = val.GetSize();
		// check if there are any non-ascii characters in there
		for (idx_t i = 0; i < len; i++) {
			if (data[i] > 127) {
				// there are! fallback to slower case
				return ConvertUnicodeValue(const_char_ptr_cast(data), len, i);
			}
		}
		// no unicode: fast path
		// directly construct the string and memcpy it
		auto result = PyUnicode_New(len, 127);
		auto result_handle = py::handle(result);
		auto target_data = PyUtil::PyUnicodeDataMutable(result_handle);
		memcpy(target_data, data, len);
		return result;
	}
	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return nullptr;
	}
};

struct BlobConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(string_t val) {
		return PyByteArray_FromStringAndSize(val.GetData(), val.GetSize());
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return nullptr;
	}
};

struct BitConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(string_t val) {
		return PyBytes_FromStringAndSize(val.GetData(), val.GetSize());
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return nullptr;
	}
};

struct UUIDConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(hugeint_t val) {
		auto &import_cache = *DuckDBPyConnection::ImportCache();
		py::handle h = import_cache.uuid().UUID()(UUID::ToString(val)).release();
		return h.ptr();
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return nullptr;
	}
};

struct ListConvert {
	static py::list ConvertValue(Vector &input, idx_t chunk_offset, const ClientProperties &client_properties) {
		auto val = input.GetValue(chunk_offset);
		auto &list_children = ListValue::GetChildren(val);
		py::list list;
		for (auto &list_elem : list_children) {
			list.append(PythonObject::FromValue(list_elem, ListType::GetChildType(input.GetType()), client_properties));
		}
		return list;
	}
};

struct StructConvert {
	static py::dict ConvertValue(Vector &input, idx_t chunk_offset, const ClientProperties &client_properties) {
		py::dict py_struct;
		auto val = input.GetValue(chunk_offset);
		auto &child_types = StructType::GetChildTypes(input.GetType());
		auto &struct_children = StructValue::GetChildren(val);

		for (idx_t i = 0; i < struct_children.size(); i++) {
			auto &child_entry = child_types[i];
			auto &child_name = child_entry.first;
			auto &child_type = child_entry.second;
			py_struct[child_name.c_str()] = PythonObject::FromValue(struct_children[i], child_type, client_properties);
		}
		return py_struct;
	}
};

struct UnionConvert {
	static py::object ConvertValue(Vector &input, idx_t chunk_offset, const ClientProperties &client_properties) {
		auto val = input.GetValue(chunk_offset);
		auto value = UnionValue::GetValue(val);

		return PythonObject::FromValue(value, UnionValue::GetType(val), client_properties);
	}
};

struct MapConvert {
	static py::dict ConvertValue(Vector &input, idx_t chunk_offset, const ClientProperties &client_properties) {
		auto val = input.GetValue(chunk_offset);
		auto &list_children = ListValue::GetChildren(val);

		auto &key_type = MapType::KeyType(input.GetType());
		auto &val_type = MapType::ValueType(input.GetType());

		py::list keys;
		py::list values;
		for (auto &list_elem : list_children) {
			auto &struct_children = StructValue::GetChildren(list_elem);
			keys.append(PythonObject::FromValue(struct_children[0], key_type, client_properties));
			values.append(PythonObject::FromValue(struct_children[1], val_type, client_properties));
		}

		py::dict py_struct;
		py_struct["key"] = keys;
		py_struct["value"] = values;
		return py_struct;
	}
};

struct IntegralConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static NUMPY_T ConvertValue(DUCKDB_T val) {
		return NUMPY_T(val);
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

template <>
double IntegralConvert::ConvertValue(hugeint_t val) {
	double result;
	Hugeint::TryCast(val, result);
	return result;
}

} // namespace duckdb_py_convert

template <class DUCKDB_T, class NUMPY_T, class CONVERT>
static bool ConvertColumn(idx_t target_offset, data_ptr_t target_data, bool *target_mask, UnifiedVectorFormat &idata,
                          idx_t count) {
	auto src_ptr = UnifiedVectorFormat::GetData<DUCKDB_T>(idata);
	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	if (!idata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			if (!idata.validity.RowIsValidUnsafe(src_idx)) {
				target_mask[offset] = true;
				out_ptr[offset] = CONVERT::template NullValue<NUMPY_T>();
			} else {
				out_ptr[offset] = CONVERT::template ConvertValue<DUCKDB_T, NUMPY_T>(src_ptr[src_idx]);
				target_mask[offset] = false;
			}
		}
		return true;
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			out_ptr[offset] = CONVERT::template ConvertValue<DUCKDB_T, NUMPY_T>(src_ptr[src_idx]);
			target_mask[offset] = false;
		}
		return false;
	}
}

template <class DUCKDB_T, class NUMPY_T>
static bool ConvertColumnCategoricalTemplate(idx_t target_offset, data_ptr_t target_data, UnifiedVectorFormat &idata,
                                             idx_t count) {
	auto src_ptr = UnifiedVectorFormat::GetData<DUCKDB_T>(idata);
	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	if (!idata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			if (!idata.validity.RowIsValidUnsafe(src_idx)) {
				out_ptr[offset] = static_cast<NUMPY_T>(-1);
			} else {
				out_ptr[offset] =
				    duckdb_py_convert::RegularConvert::template ConvertValue<DUCKDB_T, NUMPY_T>(src_ptr[src_idx]);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			out_ptr[offset] =
			    duckdb_py_convert::RegularConvert::template ConvertValue<DUCKDB_T, NUMPY_T>(src_ptr[src_idx]);
		}
	}
	// Null values are encoded in the data itself
	return false;
}

template <class NUMPY_T, class CONVERT>
static bool ConvertNested(idx_t target_offset, data_ptr_t target_data, bool *target_mask, Vector &input,
                          UnifiedVectorFormat &idata, idx_t count, const ClientProperties &client_properties) {
	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	if (!idata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			if (!idata.validity.RowIsValidUnsafe(src_idx)) {
				target_mask[offset] = true;
			} else {
				out_ptr[offset] = CONVERT::ConvertValue(input, i, client_properties);
				target_mask[offset] = false;
			}
		}
		return true;
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t offset = target_offset + i;
			out_ptr[offset] = CONVERT::ConvertValue(input, i, client_properties);
			target_mask[offset] = false;
		}
		return false;
	}
}

template <class NUMPY_T>
static bool ConvertColumnCategorical(idx_t target_offset, data_ptr_t target_data, UnifiedVectorFormat &idata,
                                     idx_t count, PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::UINT8:
		return ConvertColumnCategoricalTemplate<uint8_t, NUMPY_T>(target_offset, target_data, idata, count);
	case PhysicalType::UINT16:
		return ConvertColumnCategoricalTemplate<uint16_t, NUMPY_T>(target_offset, target_data, idata, count);
	case PhysicalType::UINT32:
		return ConvertColumnCategoricalTemplate<uint32_t, NUMPY_T>(target_offset, target_data, idata, count);
	default:
		throw InternalException("Enum Physical Type not Allowed");
	}
}

template <class T>
static bool ConvertColumnRegular(idx_t target_offset, data_ptr_t target_data, bool *target_mask,
                                 UnifiedVectorFormat &idata, idx_t count) {
	return ConvertColumn<T, T, duckdb_py_convert::RegularConvert>(target_offset, target_data, target_mask, idata,
	                                                              count);
}

template <class DUCKDB_T>
static bool ConvertDecimalInternal(idx_t target_offset, data_ptr_t target_data, bool *target_mask,
                                   UnifiedVectorFormat &idata, idx_t count, double division) {
	auto src_ptr = UnifiedVectorFormat::GetData<DUCKDB_T>(idata);
	auto out_ptr = reinterpret_cast<double *>(target_data);
	if (!idata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			if (!idata.validity.RowIsValidUnsafe(src_idx)) {
				target_mask[offset] = true;
			} else {
				out_ptr[offset] =
				    duckdb_py_convert::IntegralConvert::ConvertValue<DUCKDB_T, double>(src_ptr[src_idx]) / division;
				target_mask[offset] = false;
			}
		}
		return true;
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			out_ptr[offset] =
			    duckdb_py_convert::IntegralConvert::ConvertValue<DUCKDB_T, double>(src_ptr[src_idx]) / division;
			target_mask[offset] = false;
		}
		return false;
	}
}

static bool ConvertDecimal(const LogicalType &decimal_type, idx_t target_offset, data_ptr_t target_data,
                           bool *target_mask, UnifiedVectorFormat &idata, idx_t count) {
	auto dec_scale = DecimalType::GetScale(decimal_type);
	double division = pow(10, dec_scale);
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		return ConvertDecimalInternal<int16_t>(target_offset, target_data, target_mask, idata, count, division);
	case PhysicalType::INT32:
		return ConvertDecimalInternal<int32_t>(target_offset, target_data, target_mask, idata, count, division);
	case PhysicalType::INT64:
		return ConvertDecimalInternal<int64_t>(target_offset, target_data, target_mask, idata, count, division);
	case PhysicalType::INT128:
		return ConvertDecimalInternal<hugeint_t>(target_offset, target_data, target_mask, idata, count, division);
	default:
		throw NotImplementedException("Unimplemented internal type for DECIMAL");
	}
}

RawArrayWrapper::RawArrayWrapper(const LogicalType &type) : data(nullptr), type(type), count(0) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		type_width = sizeof(bool);
		break;
	case LogicalTypeId::UTINYINT:
		type_width = sizeof(uint8_t);
		break;
	case LogicalTypeId::USMALLINT:
		type_width = sizeof(uint16_t);
		break;
	case LogicalTypeId::UINTEGER:
		type_width = sizeof(uint32_t);
		break;
	case LogicalTypeId::UBIGINT:
		type_width = sizeof(uint64_t);
		break;
	case LogicalTypeId::TINYINT:
		type_width = sizeof(int8_t);
		break;
	case LogicalTypeId::SMALLINT:
		type_width = sizeof(int16_t);
		break;
	case LogicalTypeId::INTEGER:
		type_width = sizeof(int32_t);
		break;
	case LogicalTypeId::BIGINT:
		type_width = sizeof(int64_t);
		break;
	case LogicalTypeId::FLOAT:
		type_width = sizeof(float);
		break;
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		type_width = sizeof(double);
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIMESTAMP_TZ:
		type_width = sizeof(int64_t);
		break;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BIT:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::UNION:
	case LogicalTypeId::UUID:
		type_width = sizeof(PyObject *);
		break;
	default:
		throw NotImplementedException("Unsupported type \"%s\" for DuckDB -> NumPy conversion", type.ToString());
	}
}

string RawArrayWrapper::DuckDBToNumpyDtype(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "bool";
	case LogicalTypeId::TINYINT:
		return "int8";
	case LogicalTypeId::SMALLINT:
		return "int16";
	case LogicalTypeId::INTEGER:
		return "int32";
	case LogicalTypeId::BIGINT:
		return "int64";
	case LogicalTypeId::UTINYINT:
		return "uint8";
	case LogicalTypeId::USMALLINT:
		return "uint16";
	case LogicalTypeId::UINTEGER:
		return "uint32";
	case LogicalTypeId::UBIGINT:
		return "uint64";
	case LogicalTypeId::FLOAT:
		return "float32";
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return "float64";
	case LogicalTypeId::TIMESTAMP:
		return "datetime64[us]";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "datetime64[us]";
	case LogicalTypeId::TIMESTAMP_NS:
		return "datetime64[ns]";
	case LogicalTypeId::TIMESTAMP_MS:
		return "datetime64[ms]";
	case LogicalTypeId::TIMESTAMP_SEC:
		return "datetime64[s]";
	case LogicalTypeId::DATE:
		// FIXME: should this not be 'date64[ns]' ?
		return "datetime64[ns]";
	case LogicalTypeId::INTERVAL:
		return "timedelta64[ns]";
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BIT:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::UNION:
	case LogicalTypeId::UUID:
		return "object";
	case LogicalTypeId::ENUM: {
		auto size = EnumType::GetSize(type);
		if (size <= (idx_t)NumericLimits<int8_t>::Maximum()) {
			return "int8";
		} else if (size <= (idx_t)NumericLimits<int16_t>::Maximum()) {
			return "int16";
		} else if (size <= (idx_t)NumericLimits<int32_t>::Maximum()) {
			return "int32";
		} else {
			throw InternalException("Size not supported on ENUM types");
		}
	}
	default:
		throw NotImplementedException("Unsupported type \"%s\"", type.ToString());
	}
}

void RawArrayWrapper::Initialize(idx_t capacity) {
	string dtype = DuckDBToNumpyDtype(type);

	array = py::array(py::dtype(dtype), capacity);
	data = data_ptr_cast(array.mutable_data());
}

void RawArrayWrapper::Resize(idx_t new_capacity) {
	vector<py::ssize_t> new_shape {py::ssize_t(new_capacity)};
	array.resize(new_shape, false);
	data = data_ptr_cast(array.mutable_data());
}

ArrayWrapper::ArrayWrapper(const LogicalType &type, const ClientProperties &client_properties_p)
    : requires_mask(false), client_properties(client_properties_p) {
	data = make_uniq<RawArrayWrapper>(type);
	mask = make_uniq<RawArrayWrapper>(LogicalType::BOOLEAN);
}

void ArrayWrapper::Initialize(idx_t capacity) {
	data->Initialize(capacity);
	mask->Initialize(capacity);
}

void ArrayWrapper::Resize(idx_t new_capacity) {
	data->Resize(new_capacity);
	mask->Resize(new_capacity);
}

void ArrayWrapper::Append(idx_t current_offset, Vector &input, idx_t count) {
	auto dataptr = data->data;
	auto maskptr = reinterpret_cast<bool *>(mask->data);
	D_ASSERT(dataptr);
	D_ASSERT(maskptr);
	D_ASSERT(input.GetType() == data->type);
	bool may_have_null;

	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);
	switch (input.GetType().id()) {
	case LogicalTypeId::ENUM: {
		auto size = EnumType::GetSize(input.GetType());
		auto physical_type = input.GetType().InternalType();
		if (size <= (idx_t)NumericLimits<int8_t>::Maximum()) {
			may_have_null = ConvertColumnCategorical<int8_t>(current_offset, dataptr, idata, count, physical_type);
		} else if (size <= (idx_t)NumericLimits<int16_t>::Maximum()) {
			may_have_null = ConvertColumnCategorical<int16_t>(current_offset, dataptr, idata, count, physical_type);
		} else if (size <= (idx_t)NumericLimits<int32_t>::Maximum()) {
			may_have_null = ConvertColumnCategorical<int32_t>(current_offset, dataptr, idata, count, physical_type);
		} else {
			throw InternalException("Size not supported on ENUM types");
		}
	} break;
	case LogicalTypeId::BOOLEAN:
		may_have_null = ConvertColumnRegular<bool>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::TINYINT:
		may_have_null = ConvertColumnRegular<int8_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::SMALLINT:
		may_have_null = ConvertColumnRegular<int16_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::INTEGER:
		may_have_null = ConvertColumnRegular<int32_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::BIGINT:
		may_have_null = ConvertColumnRegular<int64_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::UTINYINT:
		may_have_null = ConvertColumnRegular<uint8_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::USMALLINT:
		may_have_null = ConvertColumnRegular<uint16_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::UINTEGER:
		may_have_null = ConvertColumnRegular<uint32_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::UBIGINT:
		may_have_null = ConvertColumnRegular<uint64_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::HUGEINT:
		may_have_null = ConvertColumn<hugeint_t, double, duckdb_py_convert::IntegralConvert>(current_offset, dataptr,
		                                                                                     maskptr, idata, count);
		break;
	case LogicalTypeId::FLOAT:
		may_have_null = ConvertColumnRegular<float>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::DOUBLE:
		may_have_null = ConvertColumnRegular<double>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::DECIMAL:
		may_have_null = ConvertDecimal(input.GetType(), current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		may_have_null = ConvertColumn<timestamp_t, int64_t, duckdb_py_convert::TimestampConvertNano>(
		    current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::DATE:
		may_have_null = ConvertColumn<date_t, int64_t, duckdb_py_convert::DateConvert>(current_offset, dataptr, maskptr,
		                                                                               idata, count);
		break;
	case LogicalTypeId::TIME:
		may_have_null = ConvertColumn<dtime_t, PyObject *, duckdb_py_convert::TimeConvert>(current_offset, dataptr,
		                                                                                   maskptr, idata, count);
		break;
	case LogicalTypeId::INTERVAL:
		may_have_null = ConvertColumn<interval_t, int64_t, duckdb_py_convert::IntervalConvert>(current_offset, dataptr,
		                                                                                       maskptr, idata, count);
		break;
	case LogicalTypeId::VARCHAR:
		may_have_null = ConvertColumn<string_t, PyObject *, duckdb_py_convert::StringConvert>(current_offset, dataptr,
		                                                                                      maskptr, idata, count);
		break;
	case LogicalTypeId::BLOB:
		may_have_null = ConvertColumn<string_t, PyObject *, duckdb_py_convert::BlobConvert>(current_offset, dataptr,
		                                                                                    maskptr, idata, count);
		break;
	case LogicalTypeId::BIT:
		may_have_null = ConvertColumn<string_t, PyObject *, duckdb_py_convert::BitConvert>(current_offset, dataptr,
		                                                                                   maskptr, idata, count);
		break;
	case LogicalTypeId::LIST:
		may_have_null = ConvertNested<py::list, duckdb_py_convert::ListConvert>(current_offset, dataptr, maskptr, input,
		                                                                        idata, count, client_properties);
		break;
	case LogicalTypeId::MAP:
		may_have_null = ConvertNested<py::dict, duckdb_py_convert::MapConvert>(current_offset, dataptr, maskptr, input,
		                                                                       idata, count, client_properties);
		break;
	case LogicalTypeId::UNION:
		may_have_null = ConvertNested<py::object, duckdb_py_convert::UnionConvert>(
		    current_offset, dataptr, maskptr, input, idata, count, client_properties);
		break;
	case LogicalTypeId::STRUCT:
		may_have_null = ConvertNested<py::dict, duckdb_py_convert::StructConvert>(
		    current_offset, dataptr, maskptr, input, idata, count, client_properties);
		break;
	case LogicalTypeId::UUID:
		may_have_null = ConvertColumn<hugeint_t, PyObject *, duckdb_py_convert::UUIDConvert>(current_offset, dataptr,
		                                                                                     maskptr, idata, count);
		break;

	default:
		throw NotImplementedException("Unsupported type \"%s\"", input.GetType().ToString());
	}
	if (may_have_null) {
		requires_mask = true;
	}
	data->count += count;
	mask->count += count;
}

py::object ArrayWrapper::ToArray(idx_t count) const {
	D_ASSERT(data->array && mask->array);
	data->Resize(data->count);
	if (!requires_mask) {
		return std::move(data->array);
	}
	mask->Resize(mask->count);
	// construct numpy arrays from the data and the mask
	auto values = std::move(data->array);
	auto nullmask = std::move(mask->array);

	// create masked array and return it
	auto masked_array = py::module::import("numpy.ma").attr("masked_array")(values, nullmask);
	return masked_array;
}

NumpyResultConversion::NumpyResultConversion(const vector<LogicalType> &types, idx_t initial_capacity,
                                             const ClientProperties &client_properties)
    : count(0), capacity(0) {
	owned_data.reserve(types.size());
	for (auto &type : types) {
		owned_data.emplace_back(type, client_properties);
	}
	Resize(initial_capacity);
}

void NumpyResultConversion::Resize(idx_t new_capacity) {
	if (capacity == 0) {
		for (auto &data : owned_data) {
			data.Initialize(new_capacity);
		}
	} else {
		for (auto &data : owned_data) {
			data.Resize(new_capacity);
		}
	}
	capacity = new_capacity;
}

void NumpyResultConversion::Append(DataChunk &chunk) {
	if (count + chunk.size() > capacity) {
		Resize(capacity * 2);
	}
	auto chunk_types = chunk.GetTypes();
	for (idx_t col_idx = 0; col_idx < owned_data.size(); col_idx++) {
		owned_data[col_idx].Append(count, chunk.data[col_idx], chunk.size());
	}
	count += chunk.size();
#ifdef DEBUG
	for (auto &data : owned_data) {
		D_ASSERT(data.data->count == count);
		D_ASSERT(data.mask->count == count);
	}
#endif
}

} // namespace duckdb
