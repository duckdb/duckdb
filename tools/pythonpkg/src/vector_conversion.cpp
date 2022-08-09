#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/vector_conversion.hpp"
#include "duckdb_python/python_conversion.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb_python/pandas_type.hpp"
#include "duckdb_python/pandas_analyzer.hpp"
#include "duckdb_python/pandas_type.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

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
		if (Value::IsNan<T>(tgt_ptr[i])) {
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
bool TryCast(const py::object stuf, T &value) {
	try {
		value = stuf.cast<T>();
		return true;
	} catch (py::cast_error &e) {
		return false;
	}
}

template <typename T>
T Cast(const py::object obj) {
	return obj.cast<T>();
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
//! offset is the current row number within this vector
void ScanPandasObject(PandasColumnBindData &bind_data, PyObject *object, idx_t offset, Vector &out) {

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

void ScanPandasObjectColumn(PandasColumnBindData &bind_data, PyObject **col, idx_t count, idx_t offset, Vector &out) {
	// numpy_col is a sequential list of objects, that make up one "column" (Vector)
	out.SetVectorType(VectorType::FLAT_VECTOR);
	auto gil = make_unique<PythonGILWrapper>(); // We're creating python objects here, so we need the GIL

	for (idx_t i = 0; i < count; i++) {
		ScanPandasObject(bind_data, col[i], i, out);
	}
	gil.reset();
	VerifyTypeConstraints(out, count);
}

void VectorConversion::NumpyToDuckDB(PandasColumnBindData &bind_data, py::array &numpy_col, idx_t count, idx_t offset,
                                     Vector &out) {
	switch (bind_data.pandas_type) {
	case PandasType::BOOL:
		ScanPandasMasked<bool>(bind_data, count, offset, out);
		break;
	case PandasType::UINT_8:
		ScanPandasMasked<uint8_t>(bind_data, count, offset, out);
		break;
	case PandasType::UINT_16:
		ScanPandasMasked<uint16_t>(bind_data, count, offset, out);
		break;
	case PandasType::UINT_32:
		ScanPandasMasked<uint32_t>(bind_data, count, offset, out);
		break;
	case PandasType::UINT_64:
		ScanPandasMasked<uint64_t>(bind_data, count, offset, out);
		break;
	case PandasType::INT_8:
		ScanPandasMasked<int8_t>(bind_data, count, offset, out);
		break;
	case PandasType::INT_16:
		ScanPandasMasked<int16_t>(bind_data, count, offset, out);
		break;
	case PandasType::INT_32:
		ScanPandasMasked<int32_t>(bind_data, count, offset, out);
		break;
	case PandasType::INT_64:
		ScanPandasMasked<int64_t>(bind_data, count, offset, out);
		break;
	case PandasType::FLOAT_32:
		ScanPandasFpColumn<float>((float *)numpy_col.data(), count, offset, out);
		break;
	case PandasType::FLOAT_64:
		ScanPandasFpColumn<double>((double *)numpy_col.data(), count, offset, out);
		break;
	case PandasType::DATETIME:
	case PandasType::DATETIME_TZ: {
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
	case PandasType::TIMEDELTA: {
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
	case PandasType::OBJECT: {
		//! We have determined the underlying logical type of this object column
		// Get the source pointer of the numpy array
		auto src_ptr = (PyObject **)numpy_col.data();
		if (out.GetType().id() != LogicalTypeId::VARCHAR) {
			return ScanPandasObjectColumn(bind_data, src_ptr, count, offset, out);
		}

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
		throw std::runtime_error("Unsupported dtype num " + to_string((uint8_t)bind_data.pandas_type));
	}
}

void VectorConversion::BindPandas(const DBConfig &config, py::handle df, vector<PandasColumnBindData> &bind_columns,
                                  vector<LogicalType> &return_types, vector<string> &names) {
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
		bind_data.pandas_type = ConvertPandasType(df_types[col_idx]);
		bool column_has_mask = py::hasattr(get_fun(df_columns[col_idx]).attr("array"), "_mask");

		if (column_has_mask) {
			// masked object, fetch the internal data and mask array
			bind_data.mask = make_unique<NumPyArrayWrapper>(get_fun(df_columns[col_idx]).attr("array").attr("_mask"));
		}

		auto column = get_fun(df_columns[col_idx]);
		if (bind_data.pandas_type == PandasType::CATEGORY) {
			// for category types, we create an ENUM type for string or use the converted numpy type for the rest
			D_ASSERT(py::hasattr(column, "cat"));
			D_ASSERT(py::hasattr(column.attr("cat"), "categories"));
			auto categories = py::array(column.attr("cat").attr("categories"));
			auto categories_pd_type = ConvertPandasType(categories.attr("dtype"));
			if (categories_pd_type == PandasType::OBJECT) {
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
				bind_data.pandas_type = ConvertPandasType(numpy_type);
				duckdb_col_type = PandasToLogicalType(bind_data.pandas_type);
			}
		} else {
			auto pandas_array = get_fun(df_columns[col_idx]).attr("array");
			if (py::hasattr(pandas_array, "_data")) {
				// This means we can access the numpy array directly
				bind_data.numpy_col = get_fun(df_columns[col_idx]).attr("array").attr("_data");
			} else {
				// Otherwise we have to get it through 'to_numpy()'
				bind_data.numpy_col = py::array(column.attr("to_numpy")());
			}
			duckdb_col_type = PandasToLogicalType(bind_data.pandas_type);
		}
		// Analyze the inner data type of the 'object' column
		if (bind_data.pandas_type == PandasType::OBJECT) {
			PandasAnalyzer analyzer(config);
			if (analyzer.Analyze(get_fun(df_columns[col_idx]))) {
				duckdb_col_type = analyzer.AnalyzedType();
			}
		}

		D_ASSERT(py::hasattr(bind_data.numpy_col, "strides"));
		bind_data.numpy_stride = bind_data.numpy_col.attr("strides").attr("__getitem__")(0).cast<idx_t>();
		return_types.push_back(duckdb_col_type);
		bind_columns.push_back(move(bind_data));
	}
}

} // namespace duckdb
