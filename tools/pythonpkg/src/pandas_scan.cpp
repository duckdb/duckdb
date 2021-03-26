#include "duckdb_python/pandas_scan.hpp"
#include "duckdb_python/array_wrapper.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

enum class PandasType : uint8_t {
	BOOLEAN,
	TINYINT,
	SMALLINT,
	INTEGER,
	BIGINT,
	UTINYINT,
	USMALLINT,
	UINTEGER,
	UBIGINT,
	FLOAT,
	DOUBLE,
	TIMESTAMP,
	VARCHAR
};

struct NumPyArrayWrapper {
	explicit NumPyArrayWrapper(py::array numpy_array) : numpy_array(move(numpy_array)) {
	}

	py::array numpy_array;
};

struct PandasColumnBindData {
	PandasType pandas_type;
	py::array numpy_col;
	unique_ptr<NumPyArrayWrapper> mask;
};

struct PandasScanFunctionData : public TableFunctionData {
	PandasScanFunctionData(py::handle df, idx_t row_count, vector<PandasColumnBindData> pandas_bind_data,
	                       vector<LogicalType> sql_types)
	    : df(df), row_count(row_count), lines_read(0), pandas_bind_data(move(pandas_bind_data)),
	      sql_types(move(sql_types)) {
	}
	py::handle df;
	idx_t row_count;
	std::atomic<idx_t> lines_read;
	vector<PandasColumnBindData> pandas_bind_data;
	vector<LogicalType> sql_types;
};

struct PandasScanState : public FunctionOperatorData {
	PandasScanState(idx_t start, idx_t end) : start(start), end(end) {
	}

	idx_t start;
	idx_t end;
	vector<column_t> column_ids;
};

struct ParallelPandasScanState : public ParallelState {
	ParallelPandasScanState() : position(0) {
	}

	std::mutex lock;
	idx_t position;
};

PandasScanFunction::PandasScanFunction()
	: TableFunction("pandas_scan", {LogicalType::VARCHAR}, PandasScanFunc, PandasScanBind, PandasScanInit, nullptr,
					nullptr, nullptr, PandasScanCardinality, nullptr, nullptr, PandasScanMaxThreads,
					PandasScanInitParallelState, PandasScanParallelInit, PandasScanParallelStateNext, true, false,
					PandasProgress) {
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
	} else if (col_type == "object" || col_type == "string") {
		// this better be strings
		duckdb_col_type = LogicalType::VARCHAR;
		pandas_type = PandasType::VARCHAR;
	} else {
		throw std::runtime_error("unsupported python type " + col_type);
	}
}

unique_ptr<FunctionData> PandasScanFunction::PandasScanBind(ClientContext &context, vector<Value> &inputs,
												unordered_map<string, Value> &named_parameters,
												vector<LogicalType> &return_types, vector<string> &names) {
	// Hey, it works (TM)
	py::gil_scoped_acquire acquire;
	py::handle df((PyObject *)std::stoull(inputs[0].GetValue<string>(), nullptr, 16));

	/* TODO this fails on Python2 for some reason
	auto pandas_mod = py::module::import("pandas.core.frame");
	auto df_class = pandas_mod.attr("DataFrame");

	if (!df.get_type().is(df_class)) {
		throw Exception("parameter is not a DataFrame");
	} */

	auto df_columns = py::list(df.attr("columns"));
	auto df_types = py::list(df.attr("dtypes"));
	auto get_fun = df.attr("__getitem__");
	// TODO support masked arrays as well
	// TODO support dicts of numpy arrays as well
	if (py::len(df_columns) == 0 || py::len(df_types) == 0 || py::len(df_columns) != py::len(df_types)) {
		throw std::runtime_error("Need a DataFrame with at least one column");
	}
	vector<PandasColumnBindData> pandas_bind_data;
	for (idx_t col_idx = 0; col_idx < py::len(df_columns); col_idx++) {
		LogicalType duckdb_col_type;
		PandasColumnBindData bind_data;

		auto col_type = string(py::str(df_types[col_idx]));
		if (col_type == "Int8" || col_type == "Int16" || col_type == "Int32" || col_type == "Int64") {
			// numeric object
			// fetch the internal data and mask array
			bind_data.numpy_col = get_fun(df_columns[col_idx]).attr("array").attr("_data");
			bind_data.mask =
				make_unique<NumPyArrayWrapper>(get_fun(df_columns[col_idx]).attr("array").attr("_mask"));
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
		names.emplace_back(py::str(df_columns[col_idx]));
		return_types.push_back(duckdb_col_type);
		pandas_bind_data.push_back(move(bind_data));
	}
	idx_t row_count = py::len(get_fun(df_columns[0]));
	return make_unique<PandasScanFunctionData>(df, row_count, move(pandas_bind_data), return_types);
}

unique_ptr<FunctionOperatorData> PandasScanFunction::PandasScanInit(ClientContext &context, const FunctionData *bind_data_p,
														vector<column_t> &column_ids,
														TableFilterCollection *filters) {
	auto &bind_data = (const PandasScanFunctionData &)*bind_data_p;
	auto result = make_unique<PandasScanState>(0, bind_data.row_count);
	result->column_ids = column_ids;
	return result;
}

idx_t PandasScanFunction::PandasScanMaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (const PandasScanFunctionData &)*bind_data_p;
	return bind_data.row_count / PANDAS_PARTITION_COUNT + 1;
}

unique_ptr<ParallelState> PandasScanFunction::PandasScanInitParallelState(ClientContext &context,
																const FunctionData *bind_data_p) {
	return make_unique<ParallelPandasScanState>();
}

unique_ptr<FunctionOperatorData> PandasScanFunction::PandasScanParallelInit(ClientContext &context,
																const FunctionData *bind_data_p,
																ParallelState *state, vector<column_t> &column_ids,
																TableFilterCollection *filters) {
	auto result = make_unique<PandasScanState>(0, 0);
	result->column_ids = column_ids;
	if (!PandasScanParallelStateNext(context, bind_data_p, result.get(), state)) {
		return nullptr;
	}
	return move(result);
}

bool PandasScanFunction::PandasScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
										FunctionOperatorData *operator_state, ParallelState *parallel_state_p) {
	auto &bind_data = (const PandasScanFunctionData &)*bind_data_p;
	auto &parallel_state = (ParallelPandasScanState &)*parallel_state_p;
	auto &state = (PandasScanState &)*operator_state;

	lock_guard<mutex> parallel_lock(parallel_state.lock);
	if (parallel_state.position >= bind_data.row_count) {
		return false;
	}
	state.start = parallel_state.position;
	parallel_state.position += PANDAS_PARTITION_COUNT;
	if (parallel_state.position > bind_data.row_count) {
		parallel_state.position = bind_data.row_count;
	}
	state.end = parallel_state.position;
	return true;
}

int PandasScanFunction::PandasProgress(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (const PandasScanFunctionData &)*bind_data_p;
	if (bind_data.row_count == 0) {
		return 100;
	}
	auto percentage = bind_data.lines_read * 100 / bind_data.row_count;
	return percentage;
}

template <class T>
void ScanPandasColumn(py::array &numpy_col, idx_t count, idx_t offset, Vector &out) {
	auto src_ptr = (T *)numpy_col.data();
	FlatVector::SetData(out, (data_ptr_t)(src_ptr + offset));
}

template <class T>
void ScanPandasNumeric(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out) {
	ScanPandasColumn<T>(bind_data.numpy_col, count, offset, out);
	if (bind_data.mask) {
		auto mask = (bool *)bind_data.mask->numpy_array.data();
		for (idx_t i = 0; i < count; i++) {
			auto is_null = mask[offset + i];
			if (is_null) {
				FlatVector::SetNull(out, i, true);
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
	return result;
}

static void ConvertVector(PandasColumnBindData &bind_data, py::array &numpy_col, idx_t count, idx_t offset,
							Vector &out) {
	switch (bind_data.pandas_type) {
	case PandasType::BOOLEAN:
		ScanPandasColumn<bool>(numpy_col, count, offset, out);
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
	case PandasType::VARCHAR: {
		auto src_ptr = (PyObject **)numpy_col.data();
		auto tgt_ptr = FlatVector::GetData<string_t>(out);
		for (idx_t row = 0; row < count; row++) {
			auto source_idx = offset + row;
			auto val = src_ptr[source_idx];
#if PY_MAJOR_VERSION >= 3
			// Python 3 string representation:
			// https://github.com/python/cpython/blob/3a8fdb28794b2f19f6c8464378fb8b46bce1f5f4/Include/cpython/unicodeobject.h#L79
			if (!PyUnicode_CheckExact(val)) {
				FlatVector::SetNull(out, row, true);
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
				FlatVector::SetNull(out, row, true);
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

//! The main pandas scan function: note that this can be called in parallel without the GIL
//! hence this needs to be GIL-safe, i.e. no methods that create Python objects are allowed
void PandasScanFunction::PandasScanFunc(ClientContext &context, const FunctionData *bind_data,
							FunctionOperatorData *operator_state, DataChunk &output) {
	auto &data = (PandasScanFunctionData &)*bind_data;
	auto &state = (PandasScanState &)*operator_state;

	if (state.start >= state.end) {
		return;
	}
	idx_t this_count = std::min((idx_t)STANDARD_VECTOR_SIZE, state.end - state.start);
	output.SetCardinality(this_count);
	for (idx_t idx = 0; idx < state.column_ids.size(); idx++) {
		auto col_idx = state.column_ids[idx];
		if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
			output.data[idx].Sequence(state.start, this_count);
		} else {
			ConvertVector(data.pandas_bind_data[col_idx], data.pandas_bind_data[col_idx].numpy_col, this_count,
							state.start, output.data[idx]);
		}
	}
	state.start += this_count;
	data.lines_read += this_count;
}

unique_ptr<NodeStatistics> PandasScanFunction::PandasScanCardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &data = (PandasScanFunctionData &)*bind_data;
	return make_unique<NodeStatistics>(data.row_count, data.row_count);
}

}
