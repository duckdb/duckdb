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
	static NUMPY_T ConvertValue(DUCKDB_T val, NumpyAppendData &append_data) {
		(void)append_data;
		return (NUMPY_T)val;
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return 0;
	}
};

struct TimestampConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(timestamp_t val, NumpyAppendData &append_data) {
		(void)append_data;
		if (!Timestamp::IsFinite(val)) {
			return val.value;
		}
		return Timestamp::GetEpochNanoSeconds(val);
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return 0;
	}
};

struct TimestampConvertSec {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(timestamp_t val, NumpyAppendData &append_data) {
		(void)append_data;
		if (!Timestamp::IsFinite(val)) {
			return val.value;
		}
		return Timestamp::GetEpochNanoSeconds(Timestamp::FromEpochSeconds(val.value));
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return 0;
	}
};

struct TimestampConvertMilli {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(timestamp_t val, NumpyAppendData &append_data) {
		(void)append_data;
		if (!Timestamp::IsFinite(val)) {
			return val.value;
		}
		return Timestamp::GetEpochNanoSeconds(Timestamp::FromEpochMs(val.value));
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return 0;
	}
};

struct TimestampConvertNano {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(timestamp_t val, NumpyAppendData &append_data) {
		(void)append_data;
		return val.value;
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return 0;
	}
};

struct DateConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(date_t val, NumpyAppendData &append_data) {
		(void)append_data;
		return Date::EpochMicroseconds(val);
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return 0;
	}
};

struct IntervalConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(interval_t val, NumpyAppendData &append_data) {
		(void)append_data;
		return Interval::GetNanoseconds(val);
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return 0;
	}
};

struct TimeConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(dtime_t val, NumpyAppendData &append_data) {
		auto &client_properties = append_data.client_properties;
		auto value = Value::TIME(val);
		auto py_obj = PythonObject::FromValue(value, LogicalType::TIME, client_properties);
		// Release ownership of the PyObject* without decreasing refcount
		// this returns a handle, of which we take the ptr to get the PyObject*
		return py_obj.release().ptr();
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
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
	static PyObject *ConvertValue(string_t val, NumpyAppendData &append_data) {
		(void)append_data;
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
	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		if (PANDAS) {
			set_mask = false;
			Py_RETURN_NONE;
		}
		set_mask = true;
		return nullptr;
	}
};

struct BlobConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(string_t val, NumpyAppendData &append_data) {
		(void)append_data;
		return PyByteArray_FromStringAndSize(val.GetData(), val.GetSize());
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return nullptr;
	}
};

struct BitConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(string_t val, NumpyAppendData &append_data) {
		(void)append_data;
		return PyBytes_FromStringAndSize(val.GetData(), val.GetSize());
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return nullptr;
	}
};

struct UUIDConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(hugeint_t val, NumpyAppendData &append_data) {
		(void)append_data;
		auto &import_cache = *DuckDBPyConnection::ImportCache();
		py::handle h = import_cache.uuid.UUID()(UUID::ToString(val)).release();
		return h.ptr();
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return nullptr;
	}
};

static py::object InternalCreateList(Vector &input, idx_t total_size, idx_t offset, idx_t size,
                                     NumpyAppendData &append_data) {
	// Initialize the array we'll append the list data to
	auto &type = input.GetType();
	ArrayWrapper result(type, append_data.client_properties, append_data.pandas);
	result.Initialize(size);

	D_ASSERT(offset + size <= total_size);
	result.Append(0, input, total_size, offset, size);
	return result.ToArray();
}

struct ListConvert {
	static py::object ConvertValue(Vector &input, idx_t chunk_offset, NumpyAppendData &append_data) {
		auto &client_properties = append_data.client_properties;
		auto &list_data = append_data.idata;

		// Get the list entry information from the parent
		const auto list_sel = *list_data.sel;
		const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
		auto list_index = list_sel.get_index(chunk_offset);
		auto list_entry = list_entries[list_index];

		auto list_size = list_entry.length;
		auto list_offset = list_entry.offset;
		auto child_size = ListVector::GetListSize(input);
		auto &child_vector = ListVector::GetEntry(input);

		return InternalCreateList(child_vector, child_size, list_offset, list_size, append_data);
	}
};

struct ArrayConvert {
	static py::object ConvertValue(Vector &input, idx_t chunk_offset, NumpyAppendData &append_data) {
		auto &array_data = append_data.idata;

		// Get the list entry information from the parent
		const auto array_sel = *array_data.sel;
		auto array_index = array_sel.get_index(chunk_offset);

		auto &array_type = input.GetType();
		D_ASSERT(array_type.id() == LogicalTypeId::ARRAY);

		auto array_size = ArrayType::GetSize(array_type);
		auto array_offset = array_index * array_size;
		auto child_size = ArrayVector::GetTotalSize(input);
		auto &child_vector = ArrayVector::GetEntry(input);

		return InternalCreateList(child_vector, child_size, array_offset, array_size, append_data);
	}
};

struct StructConvert {
	static py::dict ConvertValue(Vector &input, idx_t chunk_offset, NumpyAppendData &append_data) {
		auto &client_properties = append_data.client_properties;

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
	static py::object ConvertValue(Vector &input, idx_t chunk_offset, NumpyAppendData &append_data) {
		auto &client_properties = append_data.client_properties;
		auto val = input.GetValue(chunk_offset);
		auto value = UnionValue::GetValue(val);

		return PythonObject::FromValue(value, UnionValue::GetType(val), client_properties);
	}
};

struct MapConvert {
	static py::dict ConvertValue(Vector &input, idx_t chunk_offset, NumpyAppendData &append_data) {
		auto &client_properties = append_data.client_properties;
		auto val = input.GetValue(chunk_offset);
		return PythonObject::FromValue(val, input.GetType(), client_properties);
	}
};

struct IntegralConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static NUMPY_T ConvertValue(DUCKDB_T val, NumpyAppendData &append_data) {
		(void)append_data;
		return NUMPY_T(val);
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return 0;
	}
};

template <>
double IntegralConvert::ConvertValue(hugeint_t val, NumpyAppendData &append_data) {
	(void)append_data;
	double result;
	Hugeint::TryCast(val, result);
	return result;
}

template <>
double IntegralConvert::ConvertValue(uhugeint_t val, NumpyAppendData &append_data) {
	(void)append_data;
	double result;
	Uhugeint::TryCast(val, result);
	return result;
}

} // namespace duckdb_py_convert

template <class DUCKDB_T, class NUMPY_T, class CONVERT, bool HAS_NULLS, bool PANDAS>
static bool ConvertColumnTemplated(NumpyAppendData &append_data) {
	auto target_offset = append_data.target_offset;
	auto target_data = append_data.target_data;
	auto target_mask = append_data.target_mask;
	auto &idata = append_data.idata;
	auto count = append_data.count;
	auto source_offset = append_data.source_offset;

	auto src_ptr = UnifiedVectorFormat::GetData<DUCKDB_T>(idata);
	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	bool mask_is_set = false;
	for (idx_t i = 0; i < count; i++) {
		idx_t src_idx = idata.sel->get_index(i + source_offset);
		idx_t offset = target_offset + i;
		if (HAS_NULLS && !idata.validity.RowIsValidUnsafe(src_idx)) {
			out_ptr[offset] = CONVERT::template NullValue<NUMPY_T, PANDAS>(target_mask[offset]);
			mask_is_set = mask_is_set || target_mask[offset];
		} else {
			out_ptr[offset] = CONVERT::template ConvertValue<DUCKDB_T, NUMPY_T>(src_ptr[src_idx], append_data);
			target_mask[offset] = false;
		}
	}
	return mask_is_set;
}

template <class DUCKDB_T, class NUMPY_T, class CONVERT>
static bool ConvertColumn(NumpyAppendData &append_data) {
	auto target_offset = append_data.target_offset;
	auto target_data = append_data.target_data;
	auto &idata = append_data.idata;

	auto src_ptr = UnifiedVectorFormat::GetData<DUCKDB_T>(idata);
	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	if (!idata.validity.AllValid()) {
		if (append_data.pandas) {
			return ConvertColumnTemplated<DUCKDB_T, NUMPY_T, CONVERT, /*has_nulls=*/true, /*pandas=*/true>(append_data);
		} else {
			return ConvertColumnTemplated<DUCKDB_T, NUMPY_T, CONVERT, /*has_nulls=*/true, /*pandas=*/false>(
			    append_data);
		}
	} else {
		if (append_data.pandas) {
			return ConvertColumnTemplated<DUCKDB_T, NUMPY_T, CONVERT, /*has_nulls=*/false, /*pandas=*/true>(
			    append_data);
		} else {
			return ConvertColumnTemplated<DUCKDB_T, NUMPY_T, CONVERT, /*has_nulls=*/false, /*pandas=*/false>(
			    append_data);
		}
	}
}

template <class DUCKDB_T, class NUMPY_T>
static bool ConvertColumnCategoricalTemplate(NumpyAppendData &append_data) {
	auto target_offset = append_data.target_offset;
	auto target_data = append_data.target_data;
	auto &idata = append_data.idata;
	auto count = append_data.count;
	auto source_offset = append_data.source_offset;

	auto src_ptr = UnifiedVectorFormat::GetData<DUCKDB_T>(idata);
	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	if (!idata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i + source_offset);
			idx_t offset = target_offset + i;
			if (!idata.validity.RowIsValidUnsafe(src_idx)) {
				out_ptr[offset] = static_cast<NUMPY_T>(-1);
			} else {
				out_ptr[offset] = duckdb_py_convert::RegularConvert::template ConvertValue<DUCKDB_T, NUMPY_T>(
				    src_ptr[src_idx], append_data);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i + source_offset);
			idx_t offset = target_offset + i;
			out_ptr[offset] = duckdb_py_convert::RegularConvert::template ConvertValue<DUCKDB_T, NUMPY_T>(
			    src_ptr[src_idx], append_data);
		}
	}
	// Null values are encoded in the data itself
	return false;
}

template <class NUMPY_T, class CONVERT>
static bool ConvertNested(NumpyAppendData &append_data) {
	auto target_offset = append_data.target_offset;
	auto target_data = append_data.target_data;
	auto target_mask = append_data.target_mask;
	auto &input = append_data.input;
	auto &idata = append_data.idata;
	auto &client_properties = append_data.client_properties;
	auto count = append_data.count;
	auto source_offset = append_data.source_offset;

	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	if (!idata.validity.AllValid()) {
		bool requires_mask = false;
		for (idx_t i = 0; i < count; i++) {
			idx_t index = i + source_offset;
			idx_t src_idx = idata.sel->get_index(index);
			idx_t offset = target_offset + i;
			if (!idata.validity.RowIsValidUnsafe(src_idx)) {
				out_ptr[offset] = py::none();
				requires_mask = true;
				target_mask[offset] = true;
			} else {
				out_ptr[offset] = CONVERT::ConvertValue(input, index, append_data);
				target_mask[offset] = false;
			}
		}
		return requires_mask;
	} else {
		for (idx_t i = 0; i < count; i++) {
			// NOTE: we do not apply the selection vector here,
			// because we use GetValue inside ConvertValue, which *also* applies the selection vector
			idx_t index = i + source_offset;
			idx_t offset = target_offset + i;
			out_ptr[offset] = CONVERT::ConvertValue(input, index, append_data);
			target_mask[offset] = false;
		}
		return false;
	}
}

template <class NUMPY_T>
static bool ConvertColumnCategorical(NumpyAppendData &append_data) {
	auto physical_type = append_data.physical_type;
	switch (physical_type) {
	case PhysicalType::UINT8:
		return ConvertColumnCategoricalTemplate<uint8_t, NUMPY_T>(append_data);
	case PhysicalType::UINT16:
		return ConvertColumnCategoricalTemplate<uint16_t, NUMPY_T>(append_data);
	case PhysicalType::UINT32:
		return ConvertColumnCategoricalTemplate<uint32_t, NUMPY_T>(append_data);
	default:
		throw InternalException("Enum Physical Type not Allowed");
	}
}

template <class T>
static bool ConvertColumnRegular(NumpyAppendData &append_data) {
	return ConvertColumn<T, T, duckdb_py_convert::RegularConvert>(append_data);
}

template <class DUCKDB_T>
static bool ConvertDecimalInternal(NumpyAppendData &append_data, double division) {
	auto target_offset = append_data.target_offset;
	auto target_data = append_data.target_data;
	auto target_mask = append_data.target_mask;
	auto &idata = append_data.idata;
	auto count = append_data.count;
	auto source_offset = append_data.source_offset;

	auto src_ptr = UnifiedVectorFormat::GetData<DUCKDB_T>(idata);
	auto out_ptr = reinterpret_cast<double *>(target_data);
	if (!idata.validity.AllValid()) {
		bool requires_mask = false;
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i + source_offset);
			idx_t offset = target_offset + i;
			if (!idata.validity.RowIsValidUnsafe(src_idx)) {
				requires_mask = true;
				target_mask[offset] = true;
			} else {
				out_ptr[offset] =
				    duckdb_py_convert::IntegralConvert::ConvertValue<DUCKDB_T, double>(src_ptr[src_idx], append_data) /
				    division;
				target_mask[offset] = false;
			}
		}
		return requires_mask;
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i + source_offset);
			idx_t offset = target_offset + i;
			out_ptr[offset] =
			    duckdb_py_convert::IntegralConvert::ConvertValue<DUCKDB_T, double>(src_ptr[src_idx], append_data) /
			    division;
			target_mask[offset] = false;
		}
		return false;
	}
}

static bool ConvertDecimal(NumpyAppendData &append_data) {
	auto &decimal_type = append_data.input.GetType();
	auto dec_scale = DecimalType::GetScale(decimal_type);
	double division = pow(10, dec_scale);
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		return ConvertDecimalInternal<int16_t>(append_data, division);
	case PhysicalType::INT32:
		return ConvertDecimalInternal<int32_t>(append_data, division);
	case PhysicalType::INT64:
		return ConvertDecimalInternal<int64_t>(append_data, division);
	case PhysicalType::INT128:
		return ConvertDecimalInternal<hugeint_t>(append_data, division);
	default:
		throw NotImplementedException("Unimplemented internal type for DECIMAL");
	}
}

ArrayWrapper::ArrayWrapper(const LogicalType &type, const ClientProperties &client_properties_p, bool pandas)
    : requires_mask(false), client_properties(client_properties_p), pandas(pandas) {
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

void ArrayWrapper::Append(idx_t current_offset, Vector &input, idx_t source_size, idx_t source_offset, idx_t count) {
	auto dataptr = data->data;
	auto maskptr = reinterpret_cast<bool *>(mask->data);
	D_ASSERT(dataptr);
	D_ASSERT(maskptr);
	D_ASSERT(input.GetType() == data->type);
	bool may_have_null;

	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(source_size, idata);

	if (count == DConstants::INVALID_INDEX) {
		D_ASSERT(source_size != DConstants::INVALID_INDEX);
		count = source_size;
	}

	NumpyAppendData append_data(idata, client_properties, input);
	append_data.target_offset = current_offset;
	append_data.target_data = dataptr;
	append_data.source_offset = source_offset;
	append_data.source_size = source_size;
	append_data.count = count;
	append_data.target_mask = maskptr;
	append_data.pandas = pandas;

	switch (input.GetType().id()) {
	case LogicalTypeId::ENUM: {
		auto size = EnumType::GetSize(input.GetType());
		append_data.physical_type = input.GetType().InternalType();
		if (size <= (idx_t)NumericLimits<int8_t>::Maximum()) {
			may_have_null = ConvertColumnCategorical<int8_t>(append_data);
		} else if (size <= (idx_t)NumericLimits<int16_t>::Maximum()) {
			may_have_null = ConvertColumnCategorical<int16_t>(append_data);
		} else if (size <= (idx_t)NumericLimits<int32_t>::Maximum()) {
			may_have_null = ConvertColumnCategorical<int32_t>(append_data);
		} else {
			throw InternalException("Size not supported on ENUM types");
		}
		break;
	}
	case LogicalTypeId::BOOLEAN:
		may_have_null = ConvertColumnRegular<bool>(append_data);
		break;
	case LogicalTypeId::TINYINT:
		may_have_null = ConvertColumnRegular<int8_t>(append_data);
		break;
	case LogicalTypeId::SMALLINT:
		may_have_null = ConvertColumnRegular<int16_t>(append_data);
		break;
	case LogicalTypeId::INTEGER:
		may_have_null = ConvertColumnRegular<int32_t>(append_data);
		break;
	case LogicalTypeId::BIGINT:
		may_have_null = ConvertColumnRegular<int64_t>(append_data);
		break;
	case LogicalTypeId::UTINYINT:
		may_have_null = ConvertColumnRegular<uint8_t>(append_data);
		break;
	case LogicalTypeId::USMALLINT:
		may_have_null = ConvertColumnRegular<uint16_t>(append_data);
		break;
	case LogicalTypeId::UINTEGER:
		may_have_null = ConvertColumnRegular<uint32_t>(append_data);
		break;
	case LogicalTypeId::UBIGINT:
		may_have_null = ConvertColumnRegular<uint64_t>(append_data);
		break;
	case LogicalTypeId::HUGEINT:
		may_have_null = ConvertColumn<hugeint_t, double, duckdb_py_convert::IntegralConvert>(append_data);
		break;
	case LogicalTypeId::UHUGEINT:
		may_have_null = ConvertColumn<uhugeint_t, double, duckdb_py_convert::IntegralConvert>(append_data);
		break;
	case LogicalTypeId::FLOAT:
		may_have_null = ConvertColumnRegular<float>(append_data);
		break;
	case LogicalTypeId::DOUBLE:
		may_have_null = ConvertColumnRegular<double>(append_data);
		break;
	case LogicalTypeId::DECIMAL:
		may_have_null = ConvertDecimal(append_data);
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		may_have_null = ConvertColumn<timestamp_t, int64_t, duckdb_py_convert::TimestampConvertNano>(append_data);
		break;
	case LogicalTypeId::DATE:
		may_have_null = ConvertColumn<date_t, int64_t, duckdb_py_convert::DateConvert>(append_data);
		break;
	case LogicalTypeId::TIME:
		may_have_null = ConvertColumn<dtime_t, PyObject *, duckdb_py_convert::TimeConvert>(append_data);
		break;
	case LogicalTypeId::INTERVAL:
		may_have_null = ConvertColumn<interval_t, int64_t, duckdb_py_convert::IntervalConvert>(append_data);
		break;
	case LogicalTypeId::VARCHAR:
		may_have_null = ConvertColumn<string_t, PyObject *, duckdb_py_convert::StringConvert>(append_data);
		break;
	case LogicalTypeId::BLOB:
		may_have_null = ConvertColumn<string_t, PyObject *, duckdb_py_convert::BlobConvert>(append_data);
		break;
	case LogicalTypeId::BIT:
		may_have_null = ConvertColumn<string_t, PyObject *, duckdb_py_convert::BitConvert>(append_data);
		break;
	case LogicalTypeId::LIST:
		may_have_null = ConvertNested<py::object, duckdb_py_convert::ListConvert>(append_data);
		break;
	case LogicalTypeId::ARRAY:
		may_have_null = ConvertNested<py::object, duckdb_py_convert::ArrayConvert>(append_data);
		break;
	case LogicalTypeId::MAP:
		may_have_null = ConvertNested<py::object, duckdb_py_convert::MapConvert>(append_data);
		break;
	case LogicalTypeId::UNION:
		may_have_null = ConvertNested<py::object, duckdb_py_convert::UnionConvert>(append_data);
		break;
	case LogicalTypeId::STRUCT:
		may_have_null = ConvertNested<py::object, duckdb_py_convert::StructConvert>(append_data);
		break;
	case LogicalTypeId::UUID:
		may_have_null = ConvertColumn<hugeint_t, PyObject *, duckdb_py_convert::UUIDConvert>(append_data);
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

py::object ArrayWrapper::ToArray() const {
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

} // namespace duckdb
