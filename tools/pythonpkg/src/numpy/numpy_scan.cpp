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
		FlatVector::SetData(out, data_ptr_cast(src_ptr + offset));
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

static void ApplyMask(PandasColumnBindData &bind_data, ValidityMask &validity, idx_t count, idx_t offset) {
	D_ASSERT(bind_data.mask);
	auto mask = reinterpret_cast<const bool *>(bind_data.mask->numpy_array.data());
	for (idx_t i = 0; i < count; i++) {
		auto is_null = mask[offset + i];
		if (is_null) {
			validity.SetInvalid(i);
		}
	}
}

template <class T>
void ScanNumpyMasked(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out) {
	D_ASSERT(bind_data.pandas_col->Backend() == PandasColumnBackend::NUMPY);
	auto &numpy_col = reinterpret_cast<PandasNumpyColumn &>(*bind_data.pandas_col);
	ScanNumpyColumn<T>(numpy_col.array, numpy_col.stride, offset, out, count);
	if (bind_data.mask) {
		auto &result_mask = FlatVector::Validity(out);
		ApplyMask(bind_data, result_mask, count, offset);
	}
}

template <class T>
void ScanNumpyFpColumn(PandasColumnBindData &bind_data, const T *src_ptr, idx_t stride, idx_t count, idx_t offset,
                       Vector &out) {
	auto &mask = FlatVector::Validity(out);
	if (stride == sizeof(T)) {
		FlatVector::SetData(out, (data_ptr_t)(src_ptr + offset)); // NOLINT
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
	if (bind_data.mask) {
		auto &result_mask = FlatVector::Validity(out);
		ApplyMask(bind_data, result_mask, count, offset);
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
void ScanNumpyObject(PyObject *object, idx_t offset, Vector &out) {

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
	auto invalid_reason = MapVector::CheckMapValidity(vec, count);
	switch (invalid_reason) {
	case MapInvalidReason::VALID:
		return;
	case MapInvalidReason::DUPLICATE_KEY:
		throw InvalidInputException("Dict->Map conversion failed because 'key' list contains duplicates");
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

void NumpyScan::ScanObjectColumn(PyObject **col, idx_t stride, idx_t count, idx_t offset, Vector &out) {
	// numpy_col is a sequential list of objects, that make up one "column" (Vector)
	out.SetVectorType(VectorType::FLAT_VECTOR);
	auto &mask = FlatVector::Validity(out);
	PythonGILWrapper gil; // We're creating python objects here, so we need the GIL

	if (stride == sizeof(PyObject *)) {
		auto src_ptr = col + offset;
		for (idx_t i = 0; i < count; i++) {
			ScanNumpyObject(src_ptr[i], i, out);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto src_ptr = col[stride / sizeof(PyObject *) * (i + offset)];
			ScanNumpyObject(src_ptr, i, out);
		}
	}
	VerifyTypeConstraints(out, count);
}

//! 'offset' is the offset within the column
//! 'count' is the amount of values we will convert in this batch
void NumpyScan::Scan(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out) {
	D_ASSERT(bind_data.pandas_col->Backend() == PandasColumnBackend::NUMPY);
	auto &numpy_col = reinterpret_cast<PandasNumpyColumn &>(*bind_data.pandas_col);
	auto &array = numpy_col.array;
	auto stride = numpy_col.stride;

	switch (bind_data.numpy_type.type) {
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
		ScanNumpyFpColumn<float>(bind_data, reinterpret_cast<const float *>(array.data()), numpy_col.stride, count,
		                         offset, out);
		break;
	case NumpyNullableType::FLOAT_64:
		ScanNumpyFpColumn<double>(bind_data, reinterpret_cast<const double *>(array.data()), numpy_col.stride, count,
		                          offset, out);
		break;
	case NumpyNullableType::DATETIME_NS:
	case NumpyNullableType::DATETIME_MS:
	case NumpyNullableType::DATETIME_US:
	case NumpyNullableType::DATETIME_S: {
		auto src_ptr = reinterpret_cast<const int64_t *>(array.data());
		auto tgt_ptr = FlatVector::GetData<timestamp_t>(out);
		auto &mask = FlatVector::Validity(out);

		using timestamp_convert_func = std::function<timestamp_t(int64_t)>;
		timestamp_convert_func convert_func;

		switch (bind_data.numpy_type.type) {
		case NumpyNullableType::DATETIME_NS:
			if (bind_data.numpy_type.has_timezone) {
				// Our timezone type is US, so we need to convert from NS to US
				convert_func = Timestamp::FromEpochNanoSeconds;
			} else {
				convert_func = Timestamp::FromEpochMicroSeconds;
			}
			break;
		case NumpyNullableType::DATETIME_MS:
			if (bind_data.numpy_type.has_timezone) {
				// Our timezone type is US, so we need to convert from MS to US
				convert_func = Timestamp::FromEpochMs;
			} else {
				convert_func = Timestamp::FromEpochMicroSeconds;
			}
			break;
		case NumpyNullableType::DATETIME_US:
			convert_func = Timestamp::FromEpochMicroSeconds;
			break;
		case NumpyNullableType::DATETIME_S:
			if (bind_data.numpy_type.has_timezone) {
				// Our timezone type is US, so we need to convert from S to US
				convert_func = Timestamp::FromEpochSeconds;
			} else {
				convert_func = Timestamp::FromEpochMicroSeconds;
			}
			break;
		default:
			throw NotImplementedException("Scan for datetime of this type is not supported yet");
		};

		for (idx_t row = 0; row < count; row++) {
			auto source_idx = stride / sizeof(int64_t) * (row + offset);
			if (src_ptr[source_idx] <= NumericLimits<int64_t>::Minimum()) {
				// pandas Not a Time (NaT)
				mask.SetInvalid(row);
				continue;
			}

			// Direct conversion, we've already matched the numpy type with the equivalent duckdb type
			auto input = timestamp_t(src_ptr[source_idx]);
			if (Timestamp::IsFinite(input)) {
				tgt_ptr[row] = convert_func(src_ptr[source_idx]);
			} else {
				tgt_ptr[row] = input;
			}
		}
		break;
	}
	case NumpyNullableType::TIMEDELTA: {
		auto src_ptr = reinterpret_cast<const int64_t *>(array.data());
		auto tgt_ptr = FlatVector::GetData<interval_t>(out);
		auto &mask = FlatVector::Validity(out);

		for (idx_t row = 0; row < count; row++) {
			auto source_idx = stride / sizeof(int64_t) * (row + offset);
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
	case NumpyNullableType::STRING:
	case NumpyNullableType::OBJECT: {
		// Get the source pointer of the numpy array
		auto src_ptr = (PyObject **)array.data(); // NOLINT
		const bool is_object_col = bind_data.numpy_type.type == NumpyNullableType::OBJECT;
		if (is_object_col && out.GetType().id() != LogicalTypeId::VARCHAR) {
			//! We have determined the underlying logical type of this object column
			return NumpyScan::ScanObjectColumn(src_ptr, numpy_col.stride, count, offset, out);
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
			if (!py::isinstance<py::str>(val)) {
				if (val == Py_None) {
					out_mask.SetInvalid(row);
					continue;
				}
				if (import_cache.pandas.NaT(false)) {
					// If pandas is imported, check if this is pandas.NaT
					py::handle value(val);
					if (value.is(import_cache.pandas.NaT())) {
						out_mask.SetInvalid(row);
						continue;
					}
				}
				if (import_cache.pandas.NA(false)) {
					// If pandas is imported, check if this is pandas.NA
					py::handle value(val);
					if (value.is(import_cache.pandas.NA())) {
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
						gil = make_uniq<PythonGILWrapper>();
					}
					bind_data.object_str_val.Push(std::move(py::str(val)));
					val = reinterpret_cast<PyObject *>(bind_data.object_str_val.LastAddedObject().ptr());
				}
			}
			// Python 3 string representation:
			// https://github.com/python/cpython/blob/3a8fdb28794b2f19f6c8464378fb8b46bce1f5f4/Include/cpython/unicodeobject.h#L79
			py::handle val_handle(val);
			if (!py::isinstance<py::str>(val_handle)) {
				out_mask.SetInvalid(row);
				continue;
			}
			if (PyUtil::PyUnicodeIsCompactASCII(val_handle)) {
				// ascii string: we can zero copy
				tgt_ptr[row] = string_t(PyUtil::PyUnicodeData(val_handle), PyUtil::PyUnicodeGetLength(val_handle));
			} else {
				// unicode gunk
				auto ascii_obj = reinterpret_cast<PyASCIIObject *>(val);
				auto unicode_obj = reinterpret_cast<PyCompactUnicodeObject *>(val);
				// compact unicode string: is there utf8 data available?
				if (unicode_obj->utf8) {
					// there is! zero copy
					tgt_ptr[row] = string_t(const_char_ptr_cast(unicode_obj->utf8), unicode_obj->utf8_length);
				} else if (PyUtil::PyUnicodeIsCompact(unicode_obj) &&
				           !PyUtil::PyUnicodeIsASCII(unicode_obj)) { // NOLINT
					auto kind = PyUtil::PyUnicodeKind(val_handle);
					switch (kind) {
					case PyUnicode_1BYTE_KIND:
						tgt_ptr[row] = DecodePythonUnicode<Py_UCS1>(PyUtil::PyUnicode1ByteData(val_handle),
						                                            PyUtil::PyUnicodeGetLength(val_handle), out);
						break;
					case PyUnicode_2BYTE_KIND:
						tgt_ptr[row] = DecodePythonUnicode<Py_UCS2>(PyUtil::PyUnicode2ByteData(val_handle),
						                                            PyUtil::PyUnicodeGetLength(val_handle), out);
						break;
					case PyUnicode_4BYTE_KIND:
						tgt_ptr[row] = DecodePythonUnicode<Py_UCS4>(PyUtil::PyUnicode4ByteData(val_handle),
						                                            PyUtil::PyUnicodeGetLength(val_handle), out);
						break;
					default:
						throw NotImplementedException(
						    "Unsupported typekind constant %d for Python Unicode Compact decode", kind);
					}
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
