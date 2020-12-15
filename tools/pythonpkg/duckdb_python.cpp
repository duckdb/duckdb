#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <unordered_map>
#include <vector>

#include "datetime.h" // from Python

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/arrow.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "extension/extension_helper.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "utf8proc_wrapper.hpp"

#include <random>

namespace py = pybind11;

using namespace duckdb;
using namespace std;

namespace duckdb_py_convert {

struct RegularConvert {
	template <class DUCKDB_T, class NUMPY_T> static NUMPY_T convert_value(DUCKDB_T val) {
		return (NUMPY_T)val;
	}
};

struct TimestampConvert {
	template <class DUCKDB_T, class NUMPY_T> static int64_t convert_value(timestamp_t val) {
		return val / 1000.0;
	}
};

struct DateConvert {
	template <class DUCKDB_T, class NUMPY_T> static int64_t convert_value(date_t val) {
		return Date::Epoch(val);
	}
};

struct TimeConvert {
	template <class DUCKDB_T, class NUMPY_T> static py::str convert_value(dtime_t val) {
		return py::str(duckdb::Time::ToString(val).c_str());
	}
};

struct StringConvert {
	template <class DUCKDB_T, class NUMPY_T> static py::str convert_value(string_t val) {
		return py::str(val.GetString());
	}
};

struct BlobConvert {
	template <class DUCKDB_T, class NUMPY_T> static py::str convert_value(string_t val) {
		return py::bytes(val.GetString());
	}
};

struct IntegralConvert {
	template <class DUCKDB_T, class NUMPY_T> static NUMPY_T convert_value(DUCKDB_T val) {
		return NUMPY_T(val);
	}
};

template <> double IntegralConvert::convert_value(hugeint_t val) {
	double result;
	Hugeint::TryCast(val, result);
	return result;
}

template <class DUCKDB_T, class NUMPY_T, class CONVERT>
static py::array fetch_column(string numpy_type, ChunkCollection &collection, idx_t column) {
	auto out = py::array(py::dtype(numpy_type), collection.Count());
	auto out_ptr = (NUMPY_T *)out.mutable_data();

	idx_t out_offset = 0;
	for (auto &data_chunk : collection.Chunks()) {
		auto &src = data_chunk->data[column];
		auto src_ptr = FlatVector::GetData<DUCKDB_T>(src);
		auto &nullmask = FlatVector::Nullmask(src);
		for (idx_t i = 0; i < data_chunk->size(); i++) {
			if (nullmask[i]) {
				continue;
			}
			out_ptr[i + out_offset] = CONVERT::template convert_value<DUCKDB_T, NUMPY_T>(src_ptr[i]);
		}
		out_offset += data_chunk->size();
	}
	return out;
}

template <class T> static py::array fetch_column_regular(string numpy_type, ChunkCollection &collection, idx_t column) {
	return fetch_column<T, T, RegularConvert>(numpy_type, collection, column);
}

template <class DUCKDB_T>
static void decimal_convert_internal(ChunkCollection &collection, idx_t column, double *out_ptr, double division) {
	idx_t out_offset = 0;
	for (auto &data_chunk : collection.Chunks()) {
		auto &src = data_chunk->data[column];
		auto src_ptr = FlatVector::GetData<DUCKDB_T>(src);
		auto &nullmask = FlatVector::Nullmask(src);
		for (idx_t i = 0; i < data_chunk->size(); i++) {
			if (nullmask[i]) {
				continue;
			}
			out_ptr[i + out_offset] = IntegralConvert::convert_value<DUCKDB_T, double>(src_ptr[i]) / division;
		}
		out_offset += data_chunk->size();
	}
}

static py::array fetch_column_decimal(string numpy_type, ChunkCollection &collection, idx_t column,
                                      LogicalType &decimal_type) {
	auto out = py::array(py::dtype(numpy_type), collection.Count());
	auto out_ptr = (double *)out.mutable_data();

	auto dec_scale = decimal_type.scale();
	double division = pow(10, dec_scale);
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		decimal_convert_internal<int16_t>(collection, column, out_ptr, division);
		break;
	case PhysicalType::INT32:
		decimal_convert_internal<int32_t>(collection, column, out_ptr, division);
		break;
	case PhysicalType::INT64:
		decimal_convert_internal<int64_t>(collection, column, out_ptr, division);
		break;
	case PhysicalType::INT128:
		decimal_convert_internal<hugeint_t>(collection, column, out_ptr, division);
		break;
	default:
		throw NotImplementedException("Unimplemented internal type for DECIMAL");
	}
	return out;
}

} // namespace duckdb_py_convert

namespace random_string {
static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_int_distribution<> dis(0, 15);

std::string generate() {
	std::stringstream ss;
	int i;
	ss << std::hex;
	for (i = 0; i < 16; i++) {
		ss << dis(gen);
	}
	return ss.str();
}
} // namespace random_string

enum class PandasType : uint8_t {
	BOOLEAN,
	TINYINT,
	SMALLINT,
	INTEGER,
	BIGINT,
	FLOAT,
	DOUBLE,
	TIMESTAMP,
	VARCHAR
};

struct NumPyArrayWrapper {
	NumPyArrayWrapper(py::array numpy_array) : numpy_array(move(numpy_array)) {}

	py::array numpy_array;
};

struct PandasColumnBindData {
	PandasType pandas_type;
	py::array numpy_col;
	unique_ptr<NumPyArrayWrapper> mask;
};

struct PandasScanFunctionData : public TableFunctionData {
	PandasScanFunctionData(py::handle df, idx_t row_count, vector<PandasColumnBindData> pandas_bind_data_,
	                       vector<LogicalType> sql_types_)
	    : df(df), row_count(row_count), pandas_bind_data(move(pandas_bind_data_)), sql_types(move(sql_types_)) {
	}
	py::handle df;
	idx_t row_count;
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
	ParallelPandasScanState() : position(0) {}

	std::mutex lock;
	idx_t position;
};

struct PandasScanFunction : public TableFunction {
	PandasScanFunction()
	    : TableFunction("pandas_scan", {LogicalType::VARCHAR}, pandas_scan_function, pandas_scan_bind, pandas_scan_init,
	                    nullptr, nullptr, nullptr, pandas_scan_cardinality, nullptr, nullptr,
						pandas_scan_max_threads, pandas_scan_init_parallel_state, pandas_scan_parallel_init,
						pandas_scan_parallel_state_next, true) {}

	static void ConvertPandasType(const string &col_type, LogicalType &duckdb_col_type, PandasType &pandas_type) {
		if (col_type == "bool") {
			duckdb_col_type = LogicalType::BOOLEAN;
			pandas_type = PandasType::BOOLEAN;
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
			// this better be strings
			duckdb_col_type = LogicalType::VARCHAR;
			pandas_type = PandasType::VARCHAR;
		} else {
			throw runtime_error("unsupported python type " + col_type);
		}
	}

	static unique_ptr<FunctionData> pandas_scan_bind(ClientContext &context, vector<Value> &inputs,
	                                                 unordered_map<string, Value> &named_parameters,
	                                                 vector<LogicalType> &return_types, vector<string> &names) {
		// Hey, it works (TM)
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
			throw runtime_error("Need a DataFrame with at least one column");
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
			names.push_back(string(py::str(df_columns[col_idx])));
			return_types.push_back(duckdb_col_type);
			pandas_bind_data.push_back(move(bind_data));
		}
		idx_t row_count = py::len(get_fun(df_columns[0]));
		return make_unique<PandasScanFunctionData>(df, row_count, move(pandas_bind_data), return_types);
	}

	static unique_ptr<FunctionOperatorData> pandas_scan_init(ClientContext &context, const FunctionData *bind_data_,
	                                                         vector<column_t> &column_ids,
	                                                         TableFilterSet *table_filters) {
		auto &bind_data = (const PandasScanFunctionData &)*bind_data_;
		auto result = make_unique<PandasScanState>(0, bind_data.row_count);
		result->column_ids = column_ids;
		return result;
	}

	static constexpr idx_t PANDAS_PARTITION_COUNT = 50 * STANDARD_VECTOR_SIZE;

	static idx_t pandas_scan_max_threads(ClientContext &context, const FunctionData *bind_data_) {
		auto &bind_data = (const PandasScanFunctionData &)*bind_data_;
		return bind_data.row_count / PANDAS_PARTITION_COUNT + 1;
	}

	static unique_ptr<ParallelState> pandas_scan_init_parallel_state(ClientContext &context, const FunctionData *bind_data_) {
		return make_unique<ParallelPandasScanState>();
	}

	static unique_ptr<FunctionOperatorData> pandas_scan_parallel_init(ClientContext &context, const FunctionData *bind_data_,
																	ParallelState *state, vector<column_t> &column_ids,
																	TableFilterSet *table_filters) {
		auto result = make_unique<PandasScanState>(0, 0);
		result->column_ids = column_ids;
		if (!pandas_scan_parallel_state_next(context, bind_data_, result.get(), state)) {
			return nullptr;
		}
		return move(result);
	}

	static bool pandas_scan_parallel_state_next(ClientContext &context, const FunctionData *bind_data_,
										FunctionOperatorData *operator_state, ParallelState *parallel_state_) {
		auto &bind_data = (const PandasScanFunctionData &)*bind_data_;
		auto &parallel_state = (ParallelPandasScanState &)*parallel_state_;
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

	template <class T> static void scan_pandas_column(py::array &numpy_col, idx_t count, idx_t offset, Vector &out) {
		auto src_ptr = (T *)numpy_col.data();
		FlatVector::SetData(out, (data_ptr_t)(src_ptr + offset));
	}

	template <class T>
	static void scan_pandas_numeric(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out) {
		scan_pandas_column<T>(bind_data.numpy_col, count, offset, out);
		if (bind_data.mask) {
			auto mask = (bool *) bind_data.mask->numpy_array.data();
			for(idx_t i = 0; i < count; i++) {
				auto is_null = mask[offset + i];
				if (is_null) {
					FlatVector::SetNull(out, i, true);
				}
			}
		}
	}

	template <class T> static bool ValueIsNull(T value) {
		throw runtime_error("unsupported type for ValueIsNull");
	}

	template <class T> static void scan_pandas_fp_column(T *src_ptr, idx_t count, idx_t offset, Vector &out) {
		FlatVector::SetData(out, (data_ptr_t)(src_ptr + offset));
		auto tgt_ptr = FlatVector::GetData<T>(out);
		auto &nullmask = FlatVector::Nullmask(out);
		for (idx_t i = 0; i < count; i++) {
			if (ValueIsNull(tgt_ptr[i])) {
				nullmask[i] = true;
			}
		}
	}

	template<class T>
	static string_t DecodePythonUnicode(T *codepoints, idx_t codepoint_count, Vector &out) {
		// first figure out how many bytes to allocate
		idx_t utf8_length = 0;
		for(idx_t i = 0; i < codepoint_count; i++) {
			int len = Utf8Proc::CodepointLength(int(codepoints[i]));
			D_ASSERT(len >= 1);
			utf8_length += len;
		}
		int sz;
		auto result = StringVector::EmptyString(out, utf8_length);
		auto target = result.GetDataWriteable();
		for(idx_t i = 0; i < codepoint_count; i++) {
			Utf8Proc::CodepointToUtf8(int(codepoints[i]), sz, target);
			D_ASSERT(sz >= 1);
			target += sz;
		}
		return result;
	}

	static void ConvertVector(PandasColumnBindData &bind_data, py::array &numpy_col, idx_t count, idx_t offset, Vector &out) {
		switch (bind_data.pandas_type) {
		case PandasType::BOOLEAN:
			scan_pandas_column<bool>(numpy_col, count, offset, out);
			break;
		case PandasType::TINYINT:
			scan_pandas_numeric<int8_t>(bind_data, count, offset, out);
			break;
		case PandasType::SMALLINT:
			scan_pandas_numeric<int16_t>(bind_data, count, offset, out);
			break;
		case PandasType::INTEGER:
			scan_pandas_numeric<int32_t>(bind_data, count, offset, out);
			break;
		case PandasType::BIGINT:
			scan_pandas_numeric<int64_t>(bind_data, count, offset, out);
			break;
		case PandasType::FLOAT:
			scan_pandas_fp_column<float>((float *)numpy_col.data(), count, offset,
											out);
			break;
		case PandasType::DOUBLE:
			scan_pandas_fp_column<double>((double *)numpy_col.data(), count, offset, out);
			break;
		case PandasType::TIMESTAMP: {
			auto src_ptr = (int64_t *)numpy_col.data();
			auto tgt_ptr = FlatVector::GetData<timestamp_t>(out);
			auto &nullmask = FlatVector::Nullmask(out);

			for (idx_t row = 0; row < count; row++) {
				auto source_idx = offset + row;
				if (src_ptr[source_idx] <= NumericLimits<int64_t>::Minimum()) {
					// pandas Not a Time (NaT)
					nullmask[row] = true;
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
					tgt_ptr[row] = string_t((const char*) PyUnicode_DATA(val), PyUnicode_GET_LENGTH(val));
				} else {
					// unicode gunk
					auto ascii_obj = (PyASCIIObject *) val;
					auto unicode_obj = (PyCompactUnicodeObject *) val;
					// compact unicode string: is there utf8 data available?
					if (unicode_obj->utf8) {
						// there is! zero copy
						tgt_ptr[row] = string_t((const char*) unicode_obj->utf8, unicode_obj->utf8_length);
					} else if (PyUnicode_IS_COMPACT(unicode_obj) && !PyUnicode_IS_ASCII(unicode_obj)) {
						auto kind = PyUnicode_KIND(val);
						switch(kind) {
						case PyUnicode_1BYTE_KIND:
							tgt_ptr[row] = DecodePythonUnicode<Py_UCS1>(PyUnicode_1BYTE_DATA(val), PyUnicode_GET_LENGTH(val), out);
							break;
						case PyUnicode_2BYTE_KIND:
							tgt_ptr[row] = DecodePythonUnicode<Py_UCS2>(PyUnicode_2BYTE_DATA(val), PyUnicode_GET_LENGTH(val), out);
							break;
						case PyUnicode_4BYTE_KIND:
							tgt_ptr[row] = DecodePythonUnicode<Py_UCS4>(PyUnicode_4BYTE_DATA(val), PyUnicode_GET_LENGTH(val), out);
							break;
						default:
							throw runtime_error("Unsupported typekind for Python Unicode Compact decode");
						}
					} else if (ascii_obj->state.kind == PyUnicode_WCHAR_KIND) {
						throw runtime_error("Unsupported: decode not ready legacy string");
					} else if (!PyUnicode_IS_COMPACT(unicode_obj) && ascii_obj->state.kind != PyUnicode_WCHAR_KIND) {
						throw runtime_error("Unsupported: decode ready legacy string");
					} else {
						throw runtime_error("Unsupported string type: no clue what this string is");
					}
				}
#else
				if (PyString_CheckExact(val)) {
					auto dataptr = PyString_AS_STRING(val);
					auto size = PyString_GET_SIZE(val);
					// string object: directly pass the data
					if (Utf8Proc::Analyze(dataptr, size) == UnicodeType::INVALID) {
						throw runtime_error("String does contains invalid UTF8! Please encode as UTF8 first");
					}
					tgt_ptr[row] = string_t(dataptr, size);
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
			throw runtime_error("Unsupported type " + out.type.ToString());
		}
	}

	//! The main pandas scan function: note that this can be called in parallel without the GIL
	//! hence this needs to be GIL-safe, i.e. no methods that create Python objects are allowed
	static void pandas_scan_function(ClientContext &context, const FunctionData *bind_data,
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
				ConvertVector(data.pandas_bind_data[col_idx], data.pandas_bind_data[col_idx].numpy_col, this_count, state.start, output.data[idx]);
			}
		}
		state.start += this_count;
	}

	static unique_ptr<NodeStatistics> pandas_scan_cardinality(ClientContext &context, const FunctionData *bind_data) {
		auto &data = (PandasScanFunctionData &)*bind_data;
		return make_unique<NodeStatistics>(data.row_count, data.row_count);
	}
};

template <> bool PandasScanFunction::ValueIsNull(float value);
template <> bool PandasScanFunction::ValueIsNull(double value);

template <> bool PandasScanFunction::ValueIsNull(float value) {
	return !Value::FloatIsValid(value);
}

template <> bool PandasScanFunction::ValueIsNull(double value) {
	return !Value::DoubleIsValid(value);
}

struct DuckDBPyResult {

	template <class SRC> static SRC fetch_scalar(Vector &src_vec, idx_t offset) {
		auto src_ptr = FlatVector::GetData<SRC>(src_vec);
		return src_ptr[offset];
	}

	py::object fetchone() {
		if (!result) {
			throw runtime_error("result closed");
		}
		if (!current_chunk || chunk_offset >= current_chunk->size()) {
			current_chunk = result->Fetch();
			chunk_offset = 0;
		}
		if (current_chunk->size() == 0) {
			return py::none();
		}
		py::tuple res(result->types.size());

		for (idx_t col_idx = 0; col_idx < result->types.size(); col_idx++) {
			auto &nullmask = FlatVector::Nullmask(current_chunk->data[col_idx]);
			if (nullmask[chunk_offset]) {
				res[col_idx] = py::none();
				continue;
			}
			auto val = current_chunk->data[col_idx].GetValue(chunk_offset);
			switch (result->types[col_idx].id()) {
			case LogicalTypeId::BOOLEAN:
				res[col_idx] = val.GetValue<bool>();
				break;
			case LogicalTypeId::TINYINT:
				res[col_idx] = val.GetValue<int8_t>();
				break;
			case LogicalTypeId::SMALLINT:
				res[col_idx] = val.GetValue<int16_t>();
				break;
			case LogicalTypeId::INTEGER:
				res[col_idx] = val.GetValue<int32_t>();
				break;
			case LogicalTypeId::BIGINT:
				res[col_idx] = val.GetValue<int64_t>();
				break;
			case LogicalTypeId::HUGEINT: {
				auto hugeint_str = val.GetValue<string>();
				res[col_idx] = PyLong_FromString((char *)hugeint_str.c_str(), nullptr, 10);
				break;
			}
			case LogicalTypeId::FLOAT:
				res[col_idx] = val.GetValue<float>();
				break;
			case LogicalTypeId::DOUBLE:
				res[col_idx] = val.GetValue<double>();
				break;
			case LogicalTypeId::DECIMAL:
				res[col_idx] = val.CastAs(LogicalType::DOUBLE).GetValue<double>();
				break;
			case LogicalTypeId::VARCHAR:
				res[col_idx] = val.GetValue<string>();
				break;
			case LogicalTypeId::BLOB:
				res[col_idx] = py::bytes(val.GetValue<string>());
				break;
			case LogicalTypeId::TIMESTAMP: {
				D_ASSERT(result->types[col_idx].InternalType() == PhysicalType::INT64);

				auto timestamp = val.GetValueUnsafe<int64_t>();
				int32_t year, month, day, hour, min, sec, micros;
				date_t date;
				dtime_t time;
				Timestamp::Convert(timestamp, date, time);
				Date::Convert(date, year, month, day);
				Time::Convert(time, hour, min, sec, micros);
				res[col_idx] = PyDateTime_FromDateAndTime(year, month, day, hour, min, sec, micros);
				break;
			}
			case LogicalTypeId::TIME: {
				D_ASSERT(result->types[col_idx].InternalType() == PhysicalType::INT64);

				int32_t hour, min, sec, microsec;
				auto time = val.GetValueUnsafe<int64_t>();
				duckdb::Time::Convert(time, hour, min, sec, microsec);
				res[col_idx] = PyTime_FromTime(hour, min, sec, microsec);
				break;
			}
			case LogicalTypeId::DATE: {
				D_ASSERT(result->types[col_idx].InternalType() == PhysicalType::INT32);

				auto date = val.GetValueUnsafe<int32_t>();
				int32_t year, month, day;
				duckdb::Date::Convert(date, year, month, day);
				res[col_idx] = PyDate_FromDate(year, month, day);
				break;
			}

			default:
				throw runtime_error("unsupported type: " + result->types[col_idx].ToString());
			}
		}
		chunk_offset++;
		return move(res);
	}

	py::list fetchall() {
		py::list res;
		while (true) {
			auto fres = fetchone();
			if (fres.is_none()) {
				break;
			}
			res.append(fres);
		}
		return res;
	}

	py::dict fetchnumpy() {
		if (!result) {
			throw runtime_error("result closed");
		}
		// need to materialize the result if it was streamed because we need the count :/
		MaterializedQueryResult *mres = nullptr;
		unique_ptr<QueryResult> mat_res_holder;
		if (result->type == QueryResultType::STREAM_RESULT) {
			mat_res_holder = ((StreamQueryResult *)result.get())->Materialize();
			mres = (MaterializedQueryResult *)mat_res_holder.get();
		} else {
			mres = (MaterializedQueryResult *)result.get();
		}
		D_ASSERT(mres);

		py::dict res;
		for (idx_t col_idx = 0; col_idx < mres->types.size(); col_idx++) {
			// convert the actual payload
			py::array col_res;
			switch (mres->types[col_idx].id()) {
			case LogicalTypeId::BOOLEAN:
				col_res = duckdb_py_convert::fetch_column_regular<bool>("bool", mres->collection, col_idx);
				break;
			case LogicalTypeId::TINYINT:
				col_res = duckdb_py_convert::fetch_column_regular<int8_t>("int8", mres->collection, col_idx);
				break;
			case LogicalTypeId::SMALLINT:
				col_res = duckdb_py_convert::fetch_column_regular<int16_t>("int16", mres->collection, col_idx);
				break;
			case LogicalTypeId::INTEGER:
				col_res = duckdb_py_convert::fetch_column_regular<int32_t>("int32", mres->collection, col_idx);
				break;
			case LogicalTypeId::BIGINT:
				col_res = duckdb_py_convert::fetch_column_regular<int64_t>("int64", mres->collection, col_idx);
				break;
			case LogicalTypeId::HUGEINT:
				col_res = duckdb_py_convert::fetch_column<hugeint_t, double, duckdb_py_convert::IntegralConvert>(
				    "float64", mres->collection, col_idx);
				break;
			case LogicalTypeId::FLOAT:
				col_res = duckdb_py_convert::fetch_column_regular<float>("float32", mres->collection, col_idx);
				break;
			case LogicalTypeId::DOUBLE:
				col_res = duckdb_py_convert::fetch_column_regular<double>("float64", mres->collection, col_idx);
				break;
			case LogicalTypeId::DECIMAL:
				col_res =
				    duckdb_py_convert::fetch_column_decimal("float64", mres->collection, col_idx, mres->types[col_idx]);
				break;
			case LogicalTypeId::TIMESTAMP:
				col_res = duckdb_py_convert::fetch_column<timestamp_t, int64_t, duckdb_py_convert::TimestampConvert>(
				    "datetime64[ms]", mres->collection, col_idx);
				break;
			case LogicalTypeId::DATE:
				col_res = duckdb_py_convert::fetch_column<date_t, int64_t, duckdb_py_convert::DateConvert>(
				    "datetime64[s]", mres->collection, col_idx);
				break;
			case LogicalTypeId::TIME:
				col_res = duckdb_py_convert::fetch_column<dtime_t, py::str, duckdb_py_convert::TimeConvert>(
				    "object", mres->collection, col_idx);
				break;
			case LogicalTypeId::VARCHAR:
				col_res = duckdb_py_convert::fetch_column<string_t, py::str, duckdb_py_convert::StringConvert>(
				    "object", mres->collection, col_idx);
				break;
			case LogicalTypeId::BLOB:
				col_res = duckdb_py_convert::fetch_column<string_t, py::bytes, duckdb_py_convert::BlobConvert>(
				    "object", mres->collection, col_idx);
				break;
			default:
				throw runtime_error("unsupported type " + mres->types[col_idx].ToString());
			}

			// convert the nullmask
			auto nullmask = py::array(py::dtype("bool"), mres->collection.Count());
			auto nullmask_ptr = (bool *)nullmask.mutable_data();
			idx_t out_offset = 0;
			for (auto &data_chunk : mres->collection.Chunks()) {
				auto &src_nm = FlatVector::Nullmask(data_chunk->data[col_idx]);
				for (idx_t i = 0; i < data_chunk->size(); i++) {
					nullmask_ptr[i + out_offset] = src_nm[i];
				}
				out_offset += data_chunk->size();
			}

			// create masked array and assign to output
			auto masked_array = py::module::import("numpy.ma").attr("masked_array")(col_res, nullmask);
			res[mres->names[col_idx].c_str()] = masked_array;
		}
		return res;
	}

	py::object fetchdf() {
		return py::module::import("pandas").attr("DataFrame").attr("from_dict")(fetchnumpy());
	}

	py::object fetch_arrow_table() {
		if (!result) {
			throw runtime_error("result closed");
		}

		auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");

		auto batch_import_func = pyarrow_lib_module.attr("RecordBatch").attr("_import_from_c");
		auto from_batches_func = pyarrow_lib_module.attr("Table").attr("from_batches");
		auto schema_import_func = pyarrow_lib_module.attr("Schema").attr("_import_from_c");
		ArrowSchema schema;
		result->ToArrowSchema(&schema);
		auto schema_obj = schema_import_func((uint64_t)&schema);

		py::list batches;
		while (true) {
			auto data_chunk = result->Fetch();
			if (data_chunk->size() == 0) {
				break;
			}
			ArrowArray data;
			data_chunk->ToArrowArray(&data);
			ArrowSchema schema;
			result->ToArrowSchema(&schema);
			batches.append(batch_import_func((uint64_t)&data, (uint64_t)&schema));
		}
		return from_batches_func(batches, schema_obj);
	}

	py::list description() {
		py::list desc(result->names.size());
		for (idx_t col_idx = 0; col_idx < result->names.size(); col_idx++) {
			py::tuple col_desc(7);
			col_desc[0] = py::str(result->names[col_idx]);
			col_desc[1] = py::none();
			col_desc[2] = py::none();
			col_desc[3] = py::none();
			col_desc[4] = py::none();
			col_desc[5] = py::none();
			col_desc[6] = py::none();
			desc[col_idx] = col_desc;
		}
		return desc;
	}

	void close() {
		result = nullptr;
	}
	idx_t chunk_offset = 0;

	unique_ptr<QueryResult> result;
	unique_ptr<DataChunk> current_chunk;
};

struct DuckDBPyRelation;

struct DuckDBPyConnection {
	DuckDBPyConnection *executemany(string query, py::object params = py::list()) {
		execute(query, params, true);
		return this;
	}

	~DuckDBPyConnection() {
		for (auto &element : registered_dfs) {
			unregister_df(element.first);
		}
	}

	DuckDBPyConnection *execute(string query, py::object params = py::list(), bool many = false) {
		if (!connection) {
			throw runtime_error("connection closed");
		}
		result = nullptr;

		auto statements = connection->ExtractStatements(query);
		if (statements.size() == 0) {
			// no statements to execute
			return this;
		}
		// if there are multiple statements, we directly execute the statements besides the last one
		// we only return the result of the last statement to the user, unless one of the previous statements fails
		for (idx_t i = 0; i + 1 < statements.size(); i++) {
			auto res = connection->Query(move(statements[i]));
			if (!res->success) {
				throw runtime_error(res->error);
			}
		}

		auto prep = connection->Prepare(move(statements.back()));
		if (!prep->success) {
			throw runtime_error(prep->error);
		}

		// this is a list of a list of parameters in executemany
		py::list params_set;
		if (!many) {
			params_set = py::list(1);
			params_set[0] = params;
		} else {
			params_set = params;
		}

		for (pybind11::handle single_query_params : params_set) {
			if (prep->n_param != py::len(single_query_params)) {
				throw runtime_error("Prepared statments needs " + to_string(prep->n_param) + " parameters, " +
				                    to_string(py::len(single_query_params)) + " given");
			}
			auto args = DuckDBPyConnection::transform_python_param_list(single_query_params);
			auto res = make_unique<DuckDBPyResult>();
			res->result = prep->Execute(args);
			if (!res->result->success) {
				throw runtime_error(res->result->error);
			}
			if (!many) {
				result = move(res);
			}
		}
		return this;
	}

	DuckDBPyConnection *append(string name, py::object value) {
		register_df("__append_df", value);
		return execute("INSERT INTO \"" + name + "\" SELECT * FROM __append_df");
	}

	static string ptr_to_string(void const *ptr) {
		std::ostringstream address;
		address << ptr;
		return address.str();
	}

	DuckDBPyConnection *register_df(string name, py::object value) {
		// hack alert: put the pointer address into the function call as a string
		execute("CREATE OR REPLACE VIEW \"" + name + "\" AS SELECT * FROM pandas_scan('" + ptr_to_string(value.ptr()) +
		        "')");

		// try to bind
		execute("SELECT * FROM \"" + name + "\" WHERE FALSE");

		// keep a reference
		registered_dfs[name] = value;
		return this;
	}

	unique_ptr<DuckDBPyRelation> table(string tname) {
		if (!connection) {
			throw runtime_error("connection closed");
		}
		return make_unique<DuckDBPyRelation>(connection->Table(tname));
	}

	unique_ptr<DuckDBPyRelation> values(py::object params = py::list()) {
		if (!connection) {
			throw runtime_error("connection closed");
		}
		vector<vector<Value>> values{DuckDBPyConnection::transform_python_param_list(params)};
		return make_unique<DuckDBPyRelation>(connection->Values(values));
	}

	unique_ptr<DuckDBPyRelation> view(string vname) {
		if (!connection) {
			throw runtime_error("connection closed");
		}
		return make_unique<DuckDBPyRelation>(connection->View(vname));
	}

	unique_ptr<DuckDBPyRelation> table_function(string fname, py::object params = py::list()) {
		if (!connection) {
			throw runtime_error("connection closed");
		}

		return make_unique<DuckDBPyRelation>(
		    connection->TableFunction(fname, DuckDBPyConnection::transform_python_param_list(params)));
	}

	unique_ptr<DuckDBPyRelation> from_df(py::object value) {
		if (!connection) {
			throw runtime_error("connection closed");
		};
		string name = "df_" + random_string::generate();
		registered_dfs[name] = value;
		vector<Value> params;
		params.push_back(Value(ptr_to_string(value.ptr())));
		return make_unique<DuckDBPyRelation>(connection->TableFunction("pandas_scan", params)->Alias(name));
	}

	unique_ptr<DuckDBPyRelation> from_csv_auto(string filename) {
		if (!connection) {
			throw runtime_error("connection closed");
		};
		vector<Value> params;
		params.push_back(Value(filename));
		return make_unique<DuckDBPyRelation>(connection->TableFunction("read_csv_auto", params)->Alias(filename));
	}

	unique_ptr<DuckDBPyRelation> from_parquet(string filename) {
		if (!connection) {
			throw runtime_error("connection closed");
		};
		vector<Value> params;
		params.push_back(Value(filename));
		return make_unique<DuckDBPyRelation>(connection->TableFunction("parquet_scan", params)->Alias(filename));
	}

	struct PythonTableArrowArrayStream {
		PythonTableArrowArrayStream(py::object arrow_table) : arrow_table(arrow_table) {
			stream.get_schema = PythonTableArrowArrayStream::my_stream_getschema;
			stream.get_next = PythonTableArrowArrayStream::my_stream_getnext;
			stream.release = PythonTableArrowArrayStream::my_stream_release;
			stream.get_last_error = PythonTableArrowArrayStream::my_stream_getlasterror;
			stream.private_data = this;

			batches = arrow_table.attr("to_batches")();
		}

		static int my_stream_getschema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
			D_ASSERT(stream->private_data);
			auto my_stream = (PythonTableArrowArrayStream *)stream->private_data;
			if (!stream->release) {
				my_stream->last_error = "stream was released";
				return -1;
			}
			my_stream->arrow_table.attr("schema").attr("_export_to_c")((uint64_t)out);
			return 0;
		}

		static int my_stream_getnext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
			D_ASSERT(stream->private_data);
			auto my_stream = (PythonTableArrowArrayStream *)stream->private_data;
			if (!stream->release) {
				my_stream->last_error = "stream was released";
				return -1;
			}
			if (my_stream->batch_idx >= py::len(my_stream->batches)) {
				out->release = nullptr;
				return 0;
			}
			my_stream->batches[my_stream->batch_idx++].attr("_export_to_c")((uint64_t)out);
			return 0;
		}

		static void my_stream_release(struct ArrowArrayStream *stream) {
			if (!stream->release) {
				return;
			}
			stream->release = nullptr;
			delete (PythonTableArrowArrayStream *)stream->private_data;
		}

		static const char *my_stream_getlasterror(struct ArrowArrayStream *stream) {
			if (!stream->release) {
				return "stream was released";
			}
			D_ASSERT(stream->private_data);
			auto my_stream = (PythonTableArrowArrayStream *)stream->private_data;
			return my_stream->last_error.c_str();
		}

		ArrowArrayStream stream;
		string last_error;
		py::object arrow_table;
		py::list batches;
		idx_t batch_idx = 0;
	};

	unique_ptr<DuckDBPyRelation> from_arrow_table(py::object table) {
		if (!connection) {
			throw runtime_error("connection closed");
		};

		// the following is a careful dance around having to depend on pyarrow
		if (table.is_none() || string(py::str(table.get_type().attr("__name__"))) != "Table") {
			throw runtime_error("Only arrow tables supported");
		}

		auto my_arrow_table = new PythonTableArrowArrayStream(table);
		string name = "arrow_table_" + ptr_to_string((void *)my_arrow_table);
		return make_unique<DuckDBPyRelation>(
		    connection->TableFunction("arrow_scan", {Value::POINTER((uintptr_t)my_arrow_table)})->Alias(name));
	}

	DuckDBPyConnection *unregister_df(string name) {
		registered_dfs[name] = py::none();
		return this;
	}

	DuckDBPyConnection *begin() {
		execute("BEGIN TRANSACTION");
		return this;
	}

	DuckDBPyConnection *commit() {
		if (connection->context->transaction.IsAutoCommit()) {
			return this;
		}
		execute("COMMIT");
		return this;
	}

	DuckDBPyConnection *rollback() {
		execute("ROLLBACK");
		return this;
	}

	py::object getattr(py::str key) {
		if (key.cast<string>() == "description") {
			if (!result) {
				throw runtime_error("no open result set");
			}
			return result->description();
		}
		return py::none();
	}

	void close() {
		connection = nullptr;
		database = nullptr;
		for (auto &cur : cursors) {
			cur->close();
		}
		cursors.clear();
	}

	// cursor() is stupid
	shared_ptr<DuckDBPyConnection> cursor() {
		auto res = make_shared<DuckDBPyConnection>();
		res->database = database;
		res->connection = make_unique<Connection>(*res->database);
		cursors.push_back(res);
		return res;
	}

	// these should be functions on the result but well
	py::tuple fetchone() {
		if (!result) {
			throw runtime_error("no open result set");
		}
		return result->fetchone();
	}

	py::list fetchall() {
		if (!result) {
			throw runtime_error("no open result set");
		}
		return result->fetchall();
	}

	py::dict fetchnumpy() {
		if (!result) {
			throw runtime_error("no open result set");
		}
		return result->fetchnumpy();
	}
	py::object fetchdf() {
		if (!result) {
			throw runtime_error("no open result set");
		}
		return result->fetchdf();
	}
	py::object fetcharrow() {
		if (!result) {
			throw runtime_error("no open result set");
		}
		return result->fetch_arrow_table();
	}

	static shared_ptr<DuckDBPyConnection> connect(string database, bool read_only) {
		auto res = make_shared<DuckDBPyConnection>();
		DBConfig config;
		if (read_only) {
			config.access_mode = AccessMode::READ_ONLY;
		}
		res->database = make_unique<DuckDB>(database, &config);
		ExtensionHelper::LoadAllExtensions(*res->database);
		res->connection = make_unique<Connection>(*res->database);

		PandasScanFunction scan_fun;
		CreateTableFunctionInfo info(scan_fun);

		auto &context = *res->connection->context;
		context.transaction.BeginTransaction();
		context.catalog.CreateTableFunction(context, &info);
		context.transaction.Commit();

		return res;
	}

	shared_ptr<DuckDB> database;
	unique_ptr<Connection> connection;
	unordered_map<string, py::object> registered_dfs;
	unique_ptr<DuckDBPyResult> result;
	vector<shared_ptr<DuckDBPyConnection>> cursors;

	static vector<Value> transform_python_param_list(py::handle params) {
		vector<Value> args;

		auto datetime_mod = py::module::import("datetime");
		auto datetime_date = datetime_mod.attr("date");
		auto datetime_datetime = datetime_mod.attr("datetime");
		auto datetime_time = datetime_mod.attr("time");
		auto decimal_mod = py::module::import("decimal");
		auto decimal_decimal = decimal_mod.attr("Decimal");

		for (pybind11::handle ele : params) {
			if (ele.is_none()) {
				args.push_back(Value());
			} else if (py::isinstance<py::bool_>(ele)) {
				args.push_back(Value::BOOLEAN(ele.cast<bool>()));
			} else if (py::isinstance<py::int_>(ele)) {
				args.push_back(Value::BIGINT(ele.cast<int64_t>()));
			} else if (py::isinstance<py::float_>(ele)) {
				args.push_back(Value::DOUBLE(ele.cast<double>()));
			} else if (py::isinstance<py::str>(ele)) {
				args.push_back(Value(ele.cast<string>()));
			} else if (py::isinstance(ele, decimal_decimal)) {
				args.push_back(Value(py::str(ele).cast<string>()));
			} else if (py::isinstance(ele, datetime_datetime)) {
				auto year = PyDateTime_GET_YEAR(ele.ptr());
				auto month = PyDateTime_GET_MONTH(ele.ptr());
				auto day = PyDateTime_GET_DAY(ele.ptr());
				auto hour = PyDateTime_DATE_GET_HOUR(ele.ptr());
				auto minute = PyDateTime_DATE_GET_MINUTE(ele.ptr());
				auto second = PyDateTime_DATE_GET_SECOND(ele.ptr());
				auto micros = PyDateTime_DATE_GET_MICROSECOND(ele.ptr());
				args.push_back(Value::TIMESTAMP(year, month, day, hour, minute, second, micros));
			} else if (py::isinstance(ele, datetime_time)) {
				auto hour = PyDateTime_TIME_GET_HOUR(ele.ptr());
				auto minute = PyDateTime_TIME_GET_MINUTE(ele.ptr());
				auto second = PyDateTime_TIME_GET_SECOND(ele.ptr());
				auto micros = PyDateTime_TIME_GET_MICROSECOND(ele.ptr());
				args.push_back(Value::TIME(hour, minute, second, micros));
			} else if (py::isinstance(ele, datetime_date)) {
				auto year = PyDateTime_GET_YEAR(ele.ptr());
				auto month = PyDateTime_GET_MONTH(ele.ptr());
				auto day = PyDateTime_GET_DAY(ele.ptr());
				args.push_back(Value::DATE(year, month, day));
			} else {
				throw runtime_error("unknown param type " + py::str(ele.get_type()).cast<string>());
			}
		}
		return args;
	}
};

static shared_ptr<DuckDBPyConnection> default_connection_ = nullptr;

static DuckDBPyConnection *default_connection() {
	if (!default_connection_) {
		default_connection_ = DuckDBPyConnection::connect(":memory:", false);
	}
	return default_connection_.get();
}

struct DuckDBPyRelation {

	DuckDBPyRelation(shared_ptr<Relation> rel) : rel(rel) {
	}

	static unique_ptr<DuckDBPyRelation> from_df(py::object df) {
		return default_connection()->from_df(df);
	}

	static unique_ptr<DuckDBPyRelation> values(py::object values = py::list()) {
		return default_connection()->values(values);
	}

	static unique_ptr<DuckDBPyRelation> from_csv_auto(string filename) {
		return default_connection()->from_csv_auto(filename);
	}

	static unique_ptr<DuckDBPyRelation> from_parquet(string filename) {
		return default_connection()->from_parquet(filename);
	}

	static unique_ptr<DuckDBPyRelation> from_arrow_table(py::object table) {
		return default_connection()->from_arrow_table(table);
	}

	unique_ptr<DuckDBPyRelation> project(string expr) {
		return make_unique<DuckDBPyRelation>(rel->Project(expr));
	}

	static unique_ptr<DuckDBPyRelation> project_df(py::object df, string expr) {
		return default_connection()->from_df(df)->project(expr);
	}

	unique_ptr<DuckDBPyRelation> alias(string expr) {
		return make_unique<DuckDBPyRelation>(rel->Alias(expr));
	}

	static unique_ptr<DuckDBPyRelation> alias_df(py::object df, string expr) {
		return default_connection()->from_df(df)->alias(expr);
	}

	unique_ptr<DuckDBPyRelation> filter(string expr) {
		return make_unique<DuckDBPyRelation>(rel->Filter(expr));
	}

	static unique_ptr<DuckDBPyRelation> filter_df(py::object df, string expr) {
		return default_connection()->from_df(df)->filter(expr);
	}

	unique_ptr<DuckDBPyRelation> limit(int64_t n) {
		return make_unique<DuckDBPyRelation>(rel->Limit(n));
	}

	static unique_ptr<DuckDBPyRelation> limit_df(py::object df, int64_t n) {
		return default_connection()->from_df(df)->limit(n);
	}

	unique_ptr<DuckDBPyRelation> order(string expr) {
		return make_unique<DuckDBPyRelation>(rel->Order(expr));
	}

	static unique_ptr<DuckDBPyRelation> order_df(py::object df, string expr) {
		return default_connection()->from_df(df)->order(expr);
	}

	unique_ptr<DuckDBPyRelation> aggregate(string expr, string groups = "") {
		if (groups.size() > 0) {
			return make_unique<DuckDBPyRelation>(rel->Aggregate(expr, groups));
		}
		return make_unique<DuckDBPyRelation>(rel->Aggregate(expr));
	}

	static unique_ptr<DuckDBPyRelation> aggregate_df(py::object df, string expr, string groups = "") {
		return default_connection()->from_df(df)->aggregate(expr, groups);
	}

	unique_ptr<DuckDBPyRelation> distinct() {
		return make_unique<DuckDBPyRelation>(rel->Distinct());
	}

	static unique_ptr<DuckDBPyRelation> distinct_df(py::object df) {
		return default_connection()->from_df(df)->distinct();
	}

	py::object to_df() {
		auto res = make_unique<DuckDBPyResult>();
		res->result = rel->Execute();
		if (!res->result->success) {
			throw runtime_error(res->result->error);
		}
		return res->fetchdf();
	}

	py::object to_arrow_table() {
		auto res = make_unique<DuckDBPyResult>();
		res->result = rel->Execute();
		if (!res->result->success) {
			throw runtime_error(res->result->error);
		}
		return res->fetch_arrow_table();
	}

	unique_ptr<DuckDBPyRelation> union_(DuckDBPyRelation *other) {
		return make_unique<DuckDBPyRelation>(rel->Union(other->rel));
	}

	unique_ptr<DuckDBPyRelation> except(DuckDBPyRelation *other) {
		return make_unique<DuckDBPyRelation>(rel->Except(other->rel));
	}

	unique_ptr<DuckDBPyRelation> intersect(DuckDBPyRelation *other) {
		return make_unique<DuckDBPyRelation>(rel->Intersect(other->rel));
	}

	unique_ptr<DuckDBPyRelation> join(DuckDBPyRelation *other, string condition) {
		return make_unique<DuckDBPyRelation>(rel->Join(other->rel, condition));
	}

	void write_csv(string file) {
		rel->WriteCSV(file);
	}

	static void write_csv_df(py::object df, string file) {
		return default_connection()->from_df(df)->write_csv(file);
	}

	// should this return a rel with the new view?
	unique_ptr<DuckDBPyRelation> create_view(string view_name, bool replace = true) {
		rel->CreateView(view_name, replace);
		return make_unique<DuckDBPyRelation>(rel);
	}

	static unique_ptr<DuckDBPyRelation> create_view_df(py::object df, string view_name, bool replace = true) {
		return default_connection()->from_df(df)->create_view(view_name, replace);
	}

	unique_ptr<DuckDBPyResult> query(string view_name, string sql_query) {
		auto res = make_unique<DuckDBPyResult>();
		res->result = rel->Query(view_name, sql_query);
		if (!res->result->success) {
			throw runtime_error(res->result->error);
		}
		return res;
	}

	unique_ptr<DuckDBPyResult> execute() {
		auto res = make_unique<DuckDBPyResult>();
		res->result = rel->Execute();
		if (!res->result->success) {
			throw runtime_error(res->result->error);
		}
		return res;
	}

	static unique_ptr<DuckDBPyResult> query_df(py::object df, string view_name, string sql_query) {
		return default_connection()->from_df(df)->query(view_name, sql_query);
	}

	void insert_into(string table) {
		rel->Insert(table);
	}

	void insert(py::object params = py::list()) {
		vector<vector<Value>> values{DuckDBPyConnection::transform_python_param_list(params)};
		rel->Insert(values);
	}

	void create(string table) {
		rel->Create(table);
	}

	string print() {
		return rel->ToString() + "\n---------------------\n-- Result Preview  --\n---------------------\n" +
		       rel->Limit(10)->Execute()->ToString() + "\n";
	}

	py::object getattr(py::str key) {
		auto key_s = key.cast<string>();
		if (key_s == "alias") {
			return py::str(string(rel->GetAlias()));
		} else if (key_s == "type") {
			return py::str(RelationTypeToString(rel->type));
		} else if (key_s == "columns") {
			py::list res;
			for (auto &col : rel->Columns()) {
				res.append(col.name);
			}
			return move(res);
		} else if (key_s == "types" || key_s == "dtypes") {
			py::list res;
			for (auto &col : rel->Columns()) {
				res.append(col.type.ToString());
			}
			return move(res);
		}
		return py::none();
	}

	shared_ptr<Relation> rel;
};

enum PySQLTokenType {
	PySQLTokenIdentifier = 0,
	PySQLTokenNumericConstant,
	PySQLTokenStringConstant,
	PySQLTokenOperator,
	PySQLTokenKeyword,
	PySQLTokenComment
};

static py::object py_tokenize(string query) {
	auto tokens = Parser::Tokenize(query);
	py::list result;
	for (auto &token : tokens) {
		auto tuple = py::tuple(2);
		tuple[0] = token.start;
		switch (token.type) {
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER:
			tuple[1] = PySQLTokenIdentifier;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_NUMERIC_CONSTANT:
			tuple[1] = PySQLTokenNumericConstant;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT:
			tuple[1] = PySQLTokenStringConstant;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR:
			tuple[1] = PySQLTokenOperator;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD:
			tuple[1] = PySQLTokenKeyword;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT:
			tuple[1] = PySQLTokenComment;
			break;
		}
		result.append(tuple);
	}
	return move(result);
}

PYBIND11_MODULE(duckdb, m) {
	m.doc() = "DuckDB is an embeddable SQL OLAP Database Management System";
	m.attr("__package__") = "duckdb";
	m.attr("__version__") = DuckDB::LibraryVersion();
	m.attr("__git_revision__") = DuckDB::SourceID();

	m.def("connect", &DuckDBPyConnection::connect,
	      "Create a DuckDB database instance. Can take a database file name to read/write persistent data and a "
	      "read_only flag if no changes are desired",
	      py::arg("database") = ":memory:", py::arg("read_only") = false);
	m.def("tokenize", py_tokenize,
	      "Tokenizes a SQL string, returning a list of (position, type) tuples that can be "
	      "used for e.g. syntax highlighting",
	      py::arg("query"));
	py::enum_<PySQLTokenType>(m, "token_type")
	    .value("identifier", PySQLTokenType::PySQLTokenIdentifier)
	    .value("numeric_const", PySQLTokenType::PySQLTokenNumericConstant)
	    .value("string_const", PySQLTokenType::PySQLTokenStringConstant)
	    .value("operator", PySQLTokenType::PySQLTokenOperator)
	    .value("keyword", PySQLTokenType::PySQLTokenKeyword)
	    .value("comment", PySQLTokenType::PySQLTokenComment)
	    .export_values();

	auto conn_class =
	    py::class_<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>>(m, "DuckDBPyConnection")
	        .def("cursor", &DuckDBPyConnection::cursor, "Create a duplicate of the current connection")
	        .def("duplicate", &DuckDBPyConnection::cursor, "Create a duplicate of the current connection")
	        .def("execute", &DuckDBPyConnection::execute,
	             "Execute the given SQL query, optionally using prepared statements with parameters set",
	             py::arg("query"), py::arg("parameters") = py::list(), py::arg("multiple_parameter_sets") = false)
	        .def("executemany", &DuckDBPyConnection::executemany,
	             "Execute the given prepared statement multiple times using the list of parameter sets in parameters",
	             py::arg("query"), py::arg("parameters") = py::list())
	        .def("close", &DuckDBPyConnection::close, "Close the connection")
	        .def("fetchone", &DuckDBPyConnection::fetchone, "Fetch a single row from a result following execute")
	        .def("fetchall", &DuckDBPyConnection::fetchall, "Fetch all rows from a result following execute")
	        .def("fetchnumpy", &DuckDBPyConnection::fetchnumpy,
	             "Fetch a result as list of NumPy arrays following execute")
	        .def("fetchdf", &DuckDBPyConnection::fetchdf, "Fetch a result as Data.Frame following execute()")
	        .def("df", &DuckDBPyConnection::fetchdf, "Fetch a result as Data.Frame following execute()")
	        .def("fetch_arrow_table", &DuckDBPyConnection::fetcharrow,
	             "Fetch a result as Arrow table following execute()")
	        .def("arrow", &DuckDBPyConnection::fetcharrow, "Fetch a result as Arrow table following execute()")
	        .def("begin", &DuckDBPyConnection::begin, "Start a new transaction")
	        .def("commit", &DuckDBPyConnection::commit, "Commit changes performed within a transaction")
	        .def("rollback", &DuckDBPyConnection::rollback, "Roll back changes performed within a transaction")
	        .def("append", &DuckDBPyConnection::append, "Append the passed Data.Frame to the named table",
	             py::arg("table_name"), py::arg("df"))
	        .def("register", &DuckDBPyConnection::register_df,
	             "Register the passed Data.Frame value for querying with a view", py::arg("view_name"), py::arg("df"))
	        .def("unregister", &DuckDBPyConnection::unregister_df, "Unregister the view name", py::arg("view_name"))
	        .def("table", &DuckDBPyConnection::table, "Create a relation object for the name'd table",
	             py::arg("table_name"))
	        .def("view", &DuckDBPyConnection::view, "Create a relation object for the name'd view",
	             py::arg("view_name"))
	        .def("values", &DuckDBPyConnection::values, "Create a relation object from the passed values",
	             py::arg("values"))
	        .def("table_function", &DuckDBPyConnection::table_function,
	             "Create a relation object from the name'd table function with given parameters", py::arg("name"),
	             py::arg("parameters") = py::list())
	        .def("from_df", &DuckDBPyConnection::from_df, "Create a relation object from the Data.Frame in df",
	             py::arg("df"))
	        .def("from_arrow_table", &DuckDBPyConnection::from_arrow_table,
	             "Create a relation object from an Arrow table", py::arg("table"))
	        .def("df", &DuckDBPyConnection::from_df,
	             "Create a relation object from the Data.Frame in df (alias of from_df)", py::arg("df"))
	        .def("from_csv_auto", &DuckDBPyConnection::from_csv_auto,
	             "Create a relation object from the CSV file in file_name", py::arg("file_name"))
	        .def("from_parquet", &DuckDBPyConnection::from_parquet,
	             "Create a relation object from the Parquet file in file_name", py::arg("file_name"))
	        .def("__getattr__", &DuckDBPyConnection::getattr, "Get result set attributes, mainly column names");

	py::class_<DuckDBPyResult>(m, "DuckDBPyResult")
	    .def("close", &DuckDBPyResult::close)
	    .def("fetchone", &DuckDBPyResult::fetchone)
	    .def("fetchall", &DuckDBPyResult::fetchall)
	    .def("fetchnumpy", &DuckDBPyResult::fetchnumpy)
	    .def("fetchdf", &DuckDBPyResult::fetchdf)
	    .def("fetch_df", &DuckDBPyResult::fetchdf)
	    .def("fetch_arrow_table", &DuckDBPyResult::fetch_arrow_table)
	    .def("arrow", &DuckDBPyResult::fetch_arrow_table)
	    .def("df", &DuckDBPyResult::fetchdf);

	py::class_<DuckDBPyRelation>(m, "DuckDBPyRelation")
	    .def("filter", &DuckDBPyRelation::filter, "Filter the relation object by the filter in filter_expr",
	         py::arg("filter_expr"))
	    .def("project", &DuckDBPyRelation::project, "Project the relation object by the projection in project_expr",
	         py::arg("project_expr"))
	    .def("set_alias", &DuckDBPyRelation::alias, "Rename the relation object to new alias", py::arg("alias"))
	    .def("order", &DuckDBPyRelation::order, "Reorder the relation object by order_expr", py::arg("order_expr"))
	    .def("aggregate", &DuckDBPyRelation::aggregate,
	         "Compute the aggregate aggr_expr by the optional groups group_expr on the relation", py::arg("aggr_expr"),
	         py::arg("group_expr") = "")
	    .def("union", &DuckDBPyRelation::union_,
	         "Create the set union of this relation object with another relation object in other_rel")
	    .def("except_", &DuckDBPyRelation::except,
	         "Create the set except of this relation object with another relation object in other_rel",
	         py::arg("other_rel"))
	    .def("intersect", &DuckDBPyRelation::intersect,
	         "Create the set intersection of this relation object with another relation object in other_rel",
	         py::arg("other_rel"))
	    .def("join", &DuckDBPyRelation::join,
	         "Join the relation object with another relation object in other_rel using the join condition expression "
	         "in join_condition",
	         py::arg("other_rel"), py::arg("join_condition"))
	    .def("distinct", &DuckDBPyRelation::distinct, "Retrieve distinct rows from this relation object")
	    .def("limit", &DuckDBPyRelation::limit, "Only retrieve the first n rows from this relation object",
	         py::arg("n"))
	    .def("query", &DuckDBPyRelation::query,
	         "Run the given SQL query in sql_query on the view named virtual_table_name that refers to the relation "
	         "object",
	         py::arg("virtual_table_name"), py::arg("sql_query"))
	    .def("execute", &DuckDBPyRelation::execute, "Transform the relation into a result set")
	    .def("write_csv", &DuckDBPyRelation::write_csv, "Write the relation object to a CSV file in file_name",
	         py::arg("file_name"))
	    .def("insert_into", &DuckDBPyRelation::insert_into,
	         "Inserts the relation object into an existing table named table_name", py::arg("table_name"))
	    .def("insert", &DuckDBPyRelation::insert, "Inserts the given values into the relation", py::arg("values"))
	    .def("create", &DuckDBPyRelation::create,
	         "Creates a new table named table_name with the contents of the relation object", py::arg("table_name"))
	    .def("create_view", &DuckDBPyRelation::create_view,
	         "Creates a view named view_name that refers to the relation object", py::arg("view_name"),
	         py::arg("replace") = true)
	    .def("to_arrow_table", &DuckDBPyRelation::to_arrow_table, "Transforms the relation object into a Arrow table")
	    .def("arrow", &DuckDBPyRelation::to_arrow_table, "Transforms the relation object into a Arrow table")
	    .def("to_df", &DuckDBPyRelation::to_df, "Transforms the relation object into a Data.Frame")
	    .def("df", &DuckDBPyRelation::to_df, "Transforms the relation object into a Data.Frame")
	    .def("__str__", &DuckDBPyRelation::print)
	    .def("__repr__", &DuckDBPyRelation::print)
	    .def("__getattr__", &DuckDBPyRelation::getattr);

	m.def("values", &DuckDBPyRelation::values, "Create a relation object from the passed values", py::arg("values"));
	m.def("from_csv_auto", &DuckDBPyRelation::from_csv_auto, "Creates a relation object from the CSV file in file_name",
	      py::arg("file_name"));
	m.def("from_parquet", &DuckDBPyRelation::from_parquet,
	      "Creates a relation object from the Parquet file in file_name", py::arg("file_name"));
	m.def("df", &DuckDBPyRelation::from_df, "Create a relation object from the Data.Frame df", py::arg("df"));
	m.def("from_df", &DuckDBPyRelation::from_df, "Create a relation object from the Data.Frame df", py::arg("df"));
	m.def("from_arrow_table", &DuckDBPyRelation::from_arrow_table, "Create a relation object from an Arrow table",
	      py::arg("table"));
	m.def("arrow", &DuckDBPyRelation::from_arrow_table, "Create a relation object from an Arrow table",
	      py::arg("table"));
	m.def("filter", &DuckDBPyRelation::filter_df, "Filter the Data.Frame df by the filter in filter_expr",
	      py::arg("df"), py::arg("filter_expr"));
	m.def("project", &DuckDBPyRelation::project_df, "Project the Data.Frame df by the projection in project_expr",
	      py::arg("df"), py::arg("project_expr"));
	m.def("alias", &DuckDBPyRelation::alias_df, "Create a relation from Data.Frame df with the passed alias",
	      py::arg("df"), py::arg("alias"));
	m.def("order", &DuckDBPyRelation::order_df, "Reorder the Data.Frame df by order_expr", py::arg("df"),
	      py::arg("order_expr"));
	m.def("aggregate", &DuckDBPyRelation::aggregate_df,
	      "Compute the aggregate aggr_expr by the optional groups group_expr on Data.frame df", py::arg("df"),
	      py::arg("aggr_expr"), py::arg("group_expr") = "");
	m.def("distinct", &DuckDBPyRelation::distinct_df, "Compute the distinct rows from Data.Frame df ", py::arg("df"));
	m.def("limit", &DuckDBPyRelation::limit_df, "Retrieve the first n rows from the Data.Frame df", py::arg("df"),
	      py::arg("n"));
	m.def("query", &DuckDBPyRelation::query_df,
	      "Run the given SQL query in sql_query on the view named virtual_table_name that contains the content of "
	      "Data.Frame df",
	      py::arg("df"), py::arg("virtual_table_name"), py::arg("sql_query"));
	m.def("write_csv", &DuckDBPyRelation::write_csv_df, "Write the Data.Frame df to a CSV file in file_name",
	      py::arg("df"), py::arg("file_name"));

	// we need this because otherwise we try to remove registered_dfs on shutdown when python is already dead
	auto clean_default_connection = []() { default_connection_.reset(); };
	m.add_object("_clean_default_connection", py::capsule(clean_default_connection));
	PyDateTime_IMPORT;
}
