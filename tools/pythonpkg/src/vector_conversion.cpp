#include "duckdb_python/vector_conversion.hpp"
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

template <class T>
void ScanPandasNumeric(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out) {
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
	return !Value::FloatIsValid(value);
}

template <>
bool ValueIsNull(double value) {
	return !Value::DoubleIsValid(value);
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

void VectorConversion::NumpyToDuckDB(PandasColumnBindData &bind_data, py::array &numpy_col, idx_t count, idx_t offset,
                                     Vector &out) {
	switch (bind_data.pandas_type) {
	case PandasType::BOOLEAN:
		ScanPandasColumn<bool>(numpy_col, bind_data.numpy_stride, offset, out, count);
		break;
	case PandasType::UTINYINT:
		ScanPandasNumeric<uint8_t>(bind_data, count, offset, out);
		break;
	case PandasType::USMALLINT:
		ScanPandasNumeric<uint16_t>(bind_data, count, offset, out);
		break;
	case PandasType::UINTEGER:
		ScanPandasNumeric<uint32_t>(bind_data, count, offset, out);
		break;
	case PandasType::UBIGINT:
		ScanPandasNumeric<uint64_t>(bind_data, count, offset, out);
		break;
	case PandasType::TINYINT:
		ScanPandasNumeric<int8_t>(bind_data, count, offset, out);
		break;
	case PandasType::SMALLINT:
		ScanPandasNumeric<int16_t>(bind_data, count, offset, out);
		break;
	case PandasType::INTEGER:
		ScanPandasNumeric<int32_t>(bind_data, count, offset, out);
		break;
	case PandasType::BIGINT:
		ScanPandasNumeric<int64_t>(bind_data, count, offset, out);
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
		auto src_ptr = (PyObject **)numpy_col.data();
		auto tgt_ptr = FlatVector::GetData<string_t>(out);
		auto &out_mask = FlatVector::Validity(out);
		for (idx_t row = 0; row < count; row++) {
			auto source_idx = offset + row;
			py::str str_val;
			PyObject *val = src_ptr[source_idx];
#if PY_MAJOR_VERSION >= 3
			if (bind_data.pandas_type == PandasType::OBJECT && !PyUnicode_CheckExact(val)) {
				if (val == Py_None) {
					out_mask.SetInvalid(row);
					continue;
				}
				if (py::isinstance<py::float_>(val) && std::isnan(PyFloat_AsDouble(val))) {
					out_mask.SetInvalid(row);
					continue;
				}
				py::handle object_handle = val;
				str_val = py::str(object_handle);
				val = str_val.ptr();
			}
#endif

#if PY_MAJOR_VERSION >= 3
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
#else
			if (PyString_CheckExact(val)) {
				auto dataptr = PyString_AS_STRING(val);
				auto size = PyString_GET_SIZE(val);
				// string object: directly pass the data
				if (Utf8Proc::Analyze(dataptr, size) == UnicodeType::INVALID) {
					throw std::runtime_error("String contains invalid UTF8! Please encode as UTF8 first");
				}
				tgt_ptr[row] = string_t(dataptr, uint32_t(size));
			} else if (PyUnicode_CheckExact(val)) {
				throw std::runtime_error("Unicode is only supported in Python 3 and up.");
			} else {
				out_mask.SetInvalid(row);
				continue;
			}
#endif
		}
		break;
	}
	default:
		throw std::runtime_error("Unsupported type " + out.GetType().ToString());
	}
}

static void ConvertPandasType(const string &col_type, LogicalType &duckdb_col_type, PandasType &pandas_type) {
	if (col_type == "bool") {
		duckdb_col_type = LogicalType::BOOLEAN;
		pandas_type = PandasType::BOOLEAN;
	} else if (col_type == "uint8" || col_type == "Uint8") {
		duckdb_col_type = LogicalType::UTINYINT;
		pandas_type = PandasType::UTINYINT;
	} else if (col_type == "uint16" || col_type == "Uint16") {
		duckdb_col_type = LogicalType::USMALLINT;
		pandas_type = PandasType::USMALLINT;
	} else if (col_type == "uint32" || col_type == "Uint32") {
		duckdb_col_type = LogicalType::UINTEGER;
		pandas_type = PandasType::UINTEGER;
	} else if (col_type == "uint64" || col_type == "Uint64") {
		duckdb_col_type = LogicalType::UBIGINT;
		pandas_type = PandasType::UBIGINT;
	} else if (col_type == "int8" || col_type == "Int8") {
		duckdb_col_type = LogicalType::TINYINT;
		pandas_type = PandasType::TINYINT;
	} else if (col_type == "int16" || col_type == "Int16") {
		duckdb_col_type = LogicalType::SMALLINT;
		pandas_type = PandasType::SMALLINT;
	} else if (col_type == "int32" || col_type == "Int32") {
		duckdb_col_type = LogicalType::INTEGER;
		pandas_type = PandasType::INTEGER;
	} else if (col_type == "int64" || col_type == "Int64") {
		duckdb_col_type = LogicalType::BIGINT;
		pandas_type = PandasType::BIGINT;
	} else if (col_type == "float32") {
		duckdb_col_type = LogicalType::FLOAT;
		pandas_type = PandasType::FLOAT;
	} else if (col_type == "float64") {
		duckdb_col_type = LogicalType::DOUBLE;
		pandas_type = PandasType::DOUBLE;
	} else if (col_type == "object") {
		//! this better be castable to strings
		duckdb_col_type = LogicalType::VARCHAR;
		pandas_type = PandasType::OBJECT;
	} else if (col_type == "string") {
		duckdb_col_type = LogicalType::VARCHAR;
		pandas_type = PandasType::VARCHAR;
	} else if (col_type == "timedelta64[ns]") {
		duckdb_col_type = LogicalType::INTERVAL;
		pandas_type = PandasType::INTERVAL;
	} else {
		throw std::runtime_error("unsupported python type " + col_type);
	}
}

void VectorConversion::BindPandas(py::handle df, vector<PandasColumnBindData> &bind_columns,
                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto df_columns = py::list(df.attr("columns"));
	auto df_types = py::list(df.attr("dtypes"));
	auto get_fun = df.attr("__getitem__");
	// TODO support masked arrays as well
	// TODO support dicts of numpy arrays as well
	if (py::len(df_columns) == 0 || py::len(df_types) == 0 || py::len(df_columns) != py::len(df_types)) {
		throw std::runtime_error("Need a DataFrame with at least one column");
	}

	// check if names in pandas dataframe are unique
	unordered_map<string, idx_t> pandas_column_names_map;
	py::array column_attributes = df.attr("columns").attr("values");
	for (idx_t col_idx = 0; col_idx < py::len(df_columns); col_idx++) {
		auto column_name_py = py::str(df_columns[col_idx]);
		pandas_column_names_map[column_name_py]++;
		if (pandas_column_names_map[column_name_py] > 1) {
			// If the column name is repeated we start adding _x where x is the repetition number
			string column_name = column_name_py;
			column_name += "_" + to_string(pandas_column_names_map[column_name_py] - 1);
			auto new_column_name_py = py::str(column_name);
			names.emplace_back(new_column_name_py);
			column_attributes[py::cast(col_idx)] = new_column_name_py;
			pandas_column_names_map[new_column_name_py]++;
		} else {
			names.emplace_back(column_name_py);
		}
	}

	for (idx_t col_idx = 0; col_idx < py::len(df_columns); col_idx++) {
		LogicalType duckdb_col_type;
		PandasColumnBindData bind_data;
		auto col_type = string(py::str(df_types[col_idx]));
		if (col_type == "Int8" || col_type == "Int16" || col_type == "Int32" || col_type == "Int64") {
			// numeric object
			// fetch the internal data and mask array
			bind_data.numpy_col = get_fun(df_columns[col_idx]).attr("array").attr("_data");
			bind_data.mask = make_unique<NumPyArrayWrapper>(get_fun(df_columns[col_idx]).attr("array").attr("_mask"));
			ConvertPandasType(col_type, duckdb_col_type, bind_data.pandas_type);
		} else if (StringUtil::StartsWith(col_type, "datetime64[ns") || col_type == "<M8[ns]") {
			// timestamp type
			bind_data.numpy_col = get_fun(df_columns[col_idx]).attr("array").attr("_data");
			bind_data.mask = nullptr;
			duckdb_col_type = LogicalType::TIMESTAMP;
			bind_data.pandas_type = PandasType::TIMESTAMP;
		} else {
			// regular type
			auto column = get_fun(df_columns[col_idx]);
			bind_data.numpy_col = py::array(column.attr("to_numpy")());
			bind_data.mask = nullptr;
			if (col_type == "category") {
				// for category types, we use the converted numpy type
				auto numpy_type = bind_data.numpy_col.attr("dtype");
				auto category_type = string(py::str(numpy_type));
				ConvertPandasType(category_type, duckdb_col_type, bind_data.pandas_type);
			} else {
				ConvertPandasType(col_type, duckdb_col_type, bind_data.pandas_type);
			}
		}
		D_ASSERT(py::hasattr(bind_data.numpy_col, "strides"));
		bind_data.numpy_stride = bind_data.numpy_col.attr("strides").attr("__getitem__")(0).cast<idx_t>();
		return_types.push_back(duckdb_col_type);
		bind_columns.push_back(move(bind_data));
	}
}
} // namespace duckdb
