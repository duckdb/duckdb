#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/python_conversion.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb_python/pandas/pandas_bind.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb_python/pandas/pandas_analyzer.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb_python/numpy/numpy_scan.hpp"
#include "duckdb_python/pandas/column/pandas_numpy_column.hpp"

namespace duckdb {

template <class T>
void ScanNumpyColumn(py::array &numpy_col, idx_t stride, idx_t offset, Vector &out, idx_t count) {
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
void ScanNumpyCategoryTemplated(py::array &column, idx_t offset, Vector &out, idx_t count) {
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
void ScanNumpyCategory(py::array &column, idx_t count, idx_t offset, Vector &out, string &src_type) {
	if (src_type == "int8") {
		ScanNumpyCategoryTemplated<int8_t, T>(column, offset, out, count);
	} else if (src_type == "int16") {
		ScanNumpyCategoryTemplated<int16_t, T>(column, offset, out, count);
	} else if (src_type == "int32") {
		ScanNumpyCategoryTemplated<int32_t, T>(column, offset, out, count);
	} else if (src_type == "int64") {
		ScanNumpyCategoryTemplated<int64_t, T>(column, offset, out, count);
	} else {
		throw NotImplementedException("The Pandas type " + src_type + " for categorical types is not implemented yet");
	}
}

template <class T>
void ScanNumpyMasked(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out) {
	D_ASSERT(bind_data.numpy_col->Backend() == PandasColumnBackend::NUMPY);
	auto &numpy_col = (PandasNumpyColumn &)*bind_data.numpy_col;
	ScanNumpyColumn<T>(numpy_col.array, numpy_col.stride, offset, out, count);
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
void ScanNumpyFpColumn(T *src_ptr, idx_t stride, idx_t count, idx_t offset, Vector &out) {
	auto &mask = FlatVector::Validity(out);
	if (stride == sizeof(T)) {
		FlatVector::SetData(out, (data_ptr_t)(src_ptr + offset));
		// Turn NaN values into NULL
		auto tgt_ptr = FlatVector::GetData<T>(out);
		for (idx_t i = 0; i < count; i++) {
			if (Value::IsNan<T>(tgt_ptr[i])) {
				mask.SetInvalid(i);
			}
		}
	} else {
		auto tgt_ptr = FlatVector::GetData<T>(out);
		for (idx_t i = 0; i < count; i++) {
			tgt_ptr[i] = src_ptr[stride / sizeof(T) * (i + offset)];
			if (Value::IsNan<T>(tgt_ptr[i])) {
				mask.SetInvalid(i);
			}
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

static void SetInvalidRecursive(Vector &out, idx_t index) {
	auto &validity = FlatVector::Validity(out);
	validity.SetInvalid(index);
	if (out.GetType().InternalType() == PhysicalType::STRUCT) {
		auto &children = StructVector::GetEntries(out);
		for (idx_t i = 0; i < children.size(); i++) {
			SetInvalidRecursive(*children[i], index);
		}
	}
}

//! 'count' is the amount of rows in the 'out' vector
//! 'offset' is the current row number within this vector
void ScanNumpyObject(PandasColumnBindData &bind_data, PyObject *object, idx_t offset, Vector &out) {

	// handle None
	if (object == Py_None) {
		SetInvalidRecursive(out, offset);
		return;
	}

	auto val = TransformPythonValue(object, out.GetType());
	// Check if the Value type is accepted for the LogicalType of Vector
	out.SetValue(offset, val);
}

static void VerifyMapConstraints(Vector &vec, idx_t count) {
	auto invalid_reason = CheckMapValidity(vec, count);
	switch (invalid_reason) {
	case MapInvalidReason::VALID:
		return;
	case MapInvalidReason::DUPLICATE_KEY:
		throw InvalidInputException("Dict->Map conversion failed because 'key' list contains duplicates");
	case MapInvalidReason::NULL_KEY_LIST:
		throw InvalidInputException("Dict->Map conversion failed because 'key' list is None");
	case MapInvalidReason::NULL_KEY:
		throw InvalidInputException("Dict->Map conversion failed because 'key' list contains None");
	default:
		throw InvalidInputException("Option not implemented for MapInvalidReason");
	}
}

void VerifyTypeConstraints(Vector &vec, idx_t count) {
	switch (vec.GetType().id()) {
	case LogicalTypeId::MAP: {
		VerifyMapConstraints(vec, count);
		break;
	}
	default:
		return;
	}
}

void ScanNumpyObjectColumn(PandasColumnBindData &bind_data, PyObject **col, idx_t count, idx_t offset, Vector &out) {
	// numpy_col is a sequential list of objects, that make up one "column" (Vector)
	out.SetVectorType(VectorType::FLAT_VECTOR);
	{
		PythonGILWrapper gil; // We're creating python objects here, so we need the GIL
		for (idx_t i = 0; i < count; i++) {
			idx_t source_idx = offset + i;
			ScanNumpyObject(bind_data, col[source_idx], i, out);
		}
	}
	VerifyTypeConstraints(out, count);
}

//! 'offset' is the offset within the column
//! 'count' is the amount of values we will convert in this batch
void NumpyScan::Scan(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out) {
	D_ASSERT(bind_data.numpy_col->Backend() == PandasColumnBackend::NUMPY);
	auto &numpy_col = (PandasNumpyColumn &)*bind_data.numpy_col;
	auto &array = numpy_col.array;

	switch (bind_data.numpy_type) {
	case NumpyNullableType::BOOL:
		ScanNumpyMasked<bool>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::UINT_8:
		ScanNumpyMasked<uint8_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::UINT_16:
		ScanNumpyMasked<uint16_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::UINT_32:
		ScanNumpyMasked<uint32_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::UINT_64:
		ScanNumpyMasked<uint64_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::INT_8:
		ScanNumpyMasked<int8_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::INT_16:
		ScanNumpyMasked<int16_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::INT_32:
		ScanNumpyMasked<int32_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::INT_64:
		ScanNumpyMasked<int64_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::FLOAT_32:
		ScanNumpyFpColumn<float>((float *)array.data(), numpy_col.stride, count, offset, out);
		break;
	case NumpyNullableType::FLOAT_64:
		ScanNumpyFpColumn<double>((double *)array.data(), numpy_col.stride, count, offset, out);
		break;
	case NumpyNullableType::DATETIME:
	case NumpyNullableType::DATETIME_TZ: {
		auto src_ptr = (int64_t *)array.data();
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
	case NumpyNullableType::TIMEDELTA: {
		auto src_ptr = (int64_t *)array.data();
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
	case NumpyNullableType::OBJECT: {
		//! We have determined the underlying logical type of this object column
		// Get the source pointer of the numpy array
		auto src_ptr = (PyObject **)array.data();
		if (out.GetType().id() != LogicalTypeId::VARCHAR) {
			return ScanNumpyObjectColumn(bind_data, src_ptr, count, offset, out);
		}

		// Get the data pointer and the validity mask of the result vector
		auto tgt_ptr = FlatVector::GetData<string_t>(out);
		auto &out_mask = FlatVector::Validity(out);
		unique_ptr<PythonGILWrapper> gil;
		auto &import_cache = *DuckDBPyConnection::ImportCache();

		// Loop over every row of the arrays contents
		auto stride = numpy_col.stride;
		for (idx_t row = 0; row < count; row++) {
			auto source_idx = stride / sizeof(PyObject *) * (row + offset);

			// Get the pointer to the object
			PyObject *val = src_ptr[source_idx];
			if (bind_data.numpy_type == NumpyNullableType::OBJECT && !py::isinstance<py::str>(val)) {
				if (val == Py_None) {
					out_mask.SetInvalid(row);
					continue;
				}
				if (import_cache.pandas().libs.NAType.IsLoaded()) {
					// If pandas is imported, check if the type is NAType
					auto val_type = Py_TYPE(val);
					auto na_type = (PyTypeObject *)import_cache.pandas().libs.NAType().ptr();
					if (val_type == na_type) {
						out_mask.SetInvalid(row);
						continue;
					}
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
			if (!py::isinstance<py::str>(val)) {
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
						throw NotImplementedException(
						    "Unsupported typekind constant %d for Python Unicode Compact decode", kind);
					}
				} else if (ascii_obj->state.kind == PyUnicode_WCHAR_KIND) {
					throw InvalidInputException("Unsupported: decode not ready legacy string");
				} else if (!PyUnicode_IS_COMPACT(unicode_obj) && ascii_obj->state.kind != PyUnicode_WCHAR_KIND) {
					throw InvalidInputException("Unsupported: decode ready legacy string");
				} else {
					throw InvalidInputException("Unsupported string type: no clue what this string is");
				}
			}
		}
		break;
	}
	case NumpyNullableType::CATEGORY: {
		switch (out.GetType().InternalType()) {
		case PhysicalType::UINT8:
			ScanNumpyCategory<uint8_t>(array, count, offset, out, bind_data.internal_categorical_type);
			break;
		case PhysicalType::UINT16:
			ScanNumpyCategory<uint16_t>(array, count, offset, out, bind_data.internal_categorical_type);
			break;
		case PhysicalType::UINT32:
			ScanNumpyCategory<uint32_t>(array, count, offset, out, bind_data.internal_categorical_type);
			break;
		default:
			throw InternalException("Invalid Physical Type for ENUMs");
		}
		break;
	}

	default:
		throw NotImplementedException("Unsupported pandas type");
	}
}

} // namespace duckdb
