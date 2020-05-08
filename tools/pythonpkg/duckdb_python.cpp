#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <unordered_map>
#include <vector>

#include "datetime.h" // from Python

#include "duckdb.hpp"

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
		return Date::Epoch(Timestamp::GetDate(val)) * 1000 + (int64_t)(Timestamp::GetTime(val));
	}
};

struct DateConvert {
	template <class DUCKDB_T, class NUMPY_T> static int64_t convert_value(date_t val) {
		return Date::Epoch(val);
	}
};

struct TimeConvert {
	template <class DUCKDB_T, class NUMPY_T> static py::str convert_value(time_t val) {
		return py::str(duckdb::Time::ToString(val).c_str());
	}
};

struct StringConvert {
	template <class DUCKDB_T, class NUMPY_T> static py::str convert_value(string_t val) {
		return py::str(val.GetData());
	}
};

template <class DUCKDB_T, class NUMPY_T, class CONVERT>
static py::array fetch_column(string numpy_type, ChunkCollection &collection, idx_t column) {
	auto out = py::array(py::dtype(numpy_type), collection.count);
	auto out_ptr = (NUMPY_T *)out.mutable_data();

	idx_t out_offset = 0;
	for (auto &data_chunk : collection.chunks) {
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

}; // namespace duckdb_py_convert
// namespace duckdb_py_convert

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

struct PandasScanFunctionData : public TableFunctionData {
	PandasScanFunctionData(py::handle df, idx_t row_count, vector<SQLType> sql_types)
	    : df(df), row_count(row_count), sql_types(sql_types), position(0) {
	}
	py::handle df;
	idx_t row_count;
	vector<SQLType> sql_types;
	idx_t position;
};

struct PandasScanFunction : public TableFunction {
	PandasScanFunction()
	    : TableFunction("pandas_scan", {SQLType::VARCHAR}, pandas_scan_bind, pandas_scan_function, nullptr){};

	static unique_ptr<FunctionData> pandas_scan_bind(ClientContext &context, vector<Value> inputs,
	                                                 vector<SQLType> &return_types, vector<string> &names) {
		// Hey, it works (TM)
		py::handle df((PyObject *)std::stoull(inputs[0].GetValue<string>(), nullptr, 16));

		/* TODO this fails on Python2 for some reason
		auto pandas_mod = py::module::import("pandas.core.frame");
		auto df_class = pandas_mod.attr("DataFrame");

		if (!df.get_type().is(df_class)) {
		    throw Exception("parameter is not a DataFrame");
		} */

		auto df_names = py::list(df.attr("columns"));
		auto df_types = py::list(df.attr("dtypes"));
		// TODO support masked arrays as well
		// TODO support dicts of numpy arrays as well
		if (py::len(df_names) == 0 || py::len(df_types) == 0 || py::len(df_names) != py::len(df_types)) {
			throw runtime_error("Need a DataFrame with at least one column");
		}
		for (idx_t col_idx = 0; col_idx < py::len(df_names); col_idx++) {
			auto col_type = string(py::str(df_types[col_idx]));
			names.push_back(string(py::str(df_names[col_idx])));
			SQLType duckdb_col_type;
			if (col_type == "bool") {
				duckdb_col_type = SQLType::BOOLEAN;
			} else if (col_type == "int8") {
				duckdb_col_type = SQLType::TINYINT;
			} else if (col_type == "int16") {
				duckdb_col_type = SQLType::SMALLINT;
			} else if (col_type == "int32") {
				duckdb_col_type = SQLType::INTEGER;
			} else if (col_type == "int64") {
				duckdb_col_type = SQLType::BIGINT;
			} else if (col_type == "float32") {
				duckdb_col_type = SQLType::FLOAT;
			} else if (col_type == "float64") {
				duckdb_col_type = SQLType::DOUBLE;
			} else if (col_type == "datetime64[ns]") {
				duckdb_col_type = SQLType::TIMESTAMP;
			} else if (col_type == "object") {
				// this better be strings
				duckdb_col_type = SQLType::VARCHAR;
			} else {
				throw runtime_error("unsupported python type " + col_type);
			}
			return_types.push_back(duckdb_col_type);
		}
		idx_t row_count = py::len(df.attr("__getitem__")(df_names[0]));
		return make_unique<PandasScanFunctionData>(df, row_count, return_types);
	}

	template <class T> static void scan_pandas_column(py::array numpy_col, idx_t count, idx_t offset, Vector &out) {
		auto src_ptr = (T *)numpy_col.data();
		FlatVector::SetData(out, (data_ptr_t) (src_ptr + offset));
	}

	static void pandas_scan_function(ClientContext &context, vector<Value> &input, DataChunk &output,
	                                 FunctionData *dataptr) {
		auto &data = *((PandasScanFunctionData *)dataptr);

		if (data.position >= data.row_count) {
			return;
		}
		idx_t this_count = std::min((idx_t)STANDARD_VECTOR_SIZE, data.row_count - data.position);

		auto df_names = py::list(data.df.attr("columns"));
		auto get_fun = data.df.attr("__getitem__");

		output.SetCardinality(this_count);
		for (idx_t col_idx = 0; col_idx < output.column_count(); col_idx++) {
			auto numpy_col = py::array(get_fun(df_names[col_idx]).attr("to_numpy")());

			switch (data.sql_types[col_idx].id) {
			case SQLTypeId::BOOLEAN:
				scan_pandas_column<bool>(numpy_col, this_count, data.position, output.data[col_idx]);
				break;
			case SQLTypeId::TINYINT:
				scan_pandas_column<int8_t>(numpy_col, this_count, data.position, output.data[col_idx]);
				break;
			case SQLTypeId::SMALLINT:
				scan_pandas_column<int16_t>(numpy_col, this_count, data.position, output.data[col_idx]);
				break;
			case SQLTypeId::INTEGER:
				scan_pandas_column<int32_t>(numpy_col, this_count, data.position, output.data[col_idx]);
				break;
			case SQLTypeId::BIGINT:
				scan_pandas_column<int64_t>(numpy_col, this_count, data.position, output.data[col_idx]);
				break;
			case SQLTypeId::FLOAT:
				scan_pandas_column<float>(numpy_col, this_count, data.position, output.data[col_idx]);
				break;
			case SQLTypeId::DOUBLE:
				scan_pandas_column<double>(numpy_col, this_count, data.position, output.data[col_idx]);
				break;
			case SQLTypeId::TIMESTAMP: {
				auto src_ptr = (int64_t *)numpy_col.data();
				auto tgt_ptr = (timestamp_t *)FlatVector::GetData(output.data[col_idx]);

				for (idx_t row = 0; row < this_count; row++) {
					auto ms = src_ptr[row] / 1000000; // nanoseconds
					auto ms_per_day = (int64_t)60 * 60 * 24 * 1000;
					date_t date = Date::EpochToDate(ms / 1000);
					dtime_t time = (dtime_t)(ms % ms_per_day);
					tgt_ptr[row] = Timestamp::FromDatetime(date, time);
				}
				break;
			} break;
			case SQLTypeId::VARCHAR: {
				auto src_ptr = (PyObject **)numpy_col.data();
				auto tgt_ptr = (string_t *)FlatVector::GetData(output.data[col_idx]);

				for (idx_t row = 0; row < this_count; row++) {
					auto val = src_ptr[row + data.position];

#if PY_MAJOR_VERSION >= 3
					if (!PyUnicode_Check(val)) {
						FlatVector::SetNull(output.data[col_idx], row, true);
						continue;
					}
					if (PyUnicode_READY(val) != 0) {
						throw runtime_error("failure in PyUnicode_READY");
					}
					if (PyUnicode_KIND(val) == PyUnicode_1BYTE_KIND) {
						auto ucs1 = PyUnicode_1BYTE_DATA(val);
						auto length = PyUnicode_GET_LENGTH(val);
						tgt_ptr[row] = string_t((const char*) ucs1, length);
					} else {
						tgt_ptr[row] = StringVector::AddString(output.data[col_idx], ((py::object*) &val)->cast<string>());
					}
#else
					if (!py::isinstance<py::str>(*((py::object*) &val))) {
						FlatVector::SetNull(output.data[col_idx], row, true);
						continue;
					}

					tgt_ptr[row] = StringVector::AddString(output.data[col_idx], ((py::object*) &val)->cast<string>());
#endif
				}
				break;
			}
			default:
				throw runtime_error("Unsupported type " + SQLTypeToString(data.sql_types[col_idx]));
			}
		}
		data.position += this_count;
	}
};

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
			switch (result->sql_types[col_idx].id) {
			case SQLTypeId::BOOLEAN:
				res[col_idx] = val.GetValue<bool>();
				break;
			case SQLTypeId::TINYINT:
				res[col_idx] = val.GetValue<int8_t>();
				break;
			case SQLTypeId::SMALLINT:
				res[col_idx] = val.GetValue<int16_t>();
				break;
			case SQLTypeId::INTEGER:
				res[col_idx] = val.GetValue<int32_t>();
				break;
			case SQLTypeId::BIGINT:
				res[col_idx] = val.GetValue<int64_t>();
				break;
			case SQLTypeId::FLOAT:
				res[col_idx] = val.GetValue<float>();
				break;
			case SQLTypeId::DOUBLE:
				res[col_idx] = val.GetValue<double>();
				break;
			case SQLTypeId::VARCHAR:
				res[col_idx] = val.GetValue<string>();
				break;

			case SQLTypeId::TIMESTAMP: {
				if (result->types[col_idx] != TypeId::INT64) {
					throw runtime_error("expected int64 for timestamp");
				}
				auto timestamp = val.GetValue<int64_t>();
				auto date = Timestamp::GetDate(timestamp);
				res[col_idx] = PyDateTime_FromDateAndTime(
				    Date::ExtractYear(date), Date::ExtractMonth(date), Date::ExtractDay(date),
				    Timestamp::GetHours(timestamp), Timestamp::GetMinutes(timestamp), Timestamp::GetSeconds(timestamp),
				    Timestamp::GetMilliseconds(timestamp) * 1000 - Timestamp::GetSeconds(timestamp) * 1000000);

				break;
			}
			case SQLTypeId::TIME: {
				if (result->types[col_idx] != TypeId::INT32) {
					throw runtime_error("expected int32 for time");
				}
				int32_t hour, min, sec, msec;
				auto time = val.GetValue<int32_t>();
				duckdb::Time::Convert(time, hour, min, sec, msec);
				res[col_idx] = PyTime_FromTime(hour, min, sec, msec * 1000);
				break;
			}
			case SQLTypeId::DATE: {
				if (result->types[col_idx] != TypeId::INT32) {
					throw runtime_error("expected int32 for date");
				}
				auto date = val.GetValue<int32_t>();
				res[col_idx] = PyDate_FromDate(duckdb::Date::ExtractYear(date), duckdb::Date::ExtractMonth(date),
				                               duckdb::Date::ExtractDay(date));
				break;
			}

			default:
				throw runtime_error("unsupported type: " + SQLTypeToString(result->sql_types[col_idx]));
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
		assert(mres);

		py::dict res;
		for (idx_t col_idx = 0; col_idx < mres->types.size(); col_idx++) {
			// convert the actual payload
			py::array col_res;
			switch (mres->sql_types[col_idx].id) {
			case SQLTypeId::BOOLEAN:
				col_res = duckdb_py_convert::fetch_column_regular<bool>("bool", mres->collection, col_idx);
				break;
			case SQLTypeId::TINYINT:
				col_res = duckdb_py_convert::fetch_column_regular<int8_t>("int8", mres->collection, col_idx);
				break;
			case SQLTypeId::SMALLINT:
				col_res = duckdb_py_convert::fetch_column_regular<int16_t>("int16", mres->collection, col_idx);
				break;
			case SQLTypeId::INTEGER:
				col_res = duckdb_py_convert::fetch_column_regular<int32_t>("int32", mres->collection, col_idx);
				break;
			case SQLTypeId::BIGINT:
				col_res = duckdb_py_convert::fetch_column_regular<int64_t>("int64", mres->collection, col_idx);
				break;
			case SQLTypeId::FLOAT:
				col_res = duckdb_py_convert::fetch_column_regular<float>("float32", mres->collection, col_idx);
				break;
			case SQLTypeId::DOUBLE:
				col_res = duckdb_py_convert::fetch_column_regular<double>("float64", mres->collection, col_idx);
				break;
			case SQLTypeId::TIMESTAMP:
				col_res = duckdb_py_convert::fetch_column<timestamp_t, int64_t, duckdb_py_convert::TimestampConvert>(
				    "datetime64[ms]", mres->collection, col_idx);
				break;
			case SQLTypeId::DATE:
				col_res = duckdb_py_convert::fetch_column<date_t, int64_t, duckdb_py_convert::DateConvert>(
				    "datetime64[s]", mres->collection, col_idx);
				break;
			case SQLTypeId::TIME:
				col_res = duckdb_py_convert::fetch_column<time_t, py::str, duckdb_py_convert::TimeConvert>(
				    "object", mres->collection, col_idx);
				break;
			case SQLTypeId::VARCHAR:
				col_res = duckdb_py_convert::fetch_column<string_t, py::str, duckdb_py_convert::StringConvert>(
				    "object", mres->collection, col_idx);
				break;
			default:
				throw runtime_error("unsupported type " + SQLTypeToString(mres->sql_types[col_idx]));
			}

			// convert the nullmask
			py::array_t<bool> nullmask;
			nullmask.resize({mres->collection.count});
			bool *nullmask_ptr = nullmask.mutable_data();

			idx_t out_offset = 0;
			for (auto &data_chunk : mres->collection.chunks) {
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

		auto prep = connection->Prepare(query);
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

		for (auto &single_query_params : params_set) {
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
		vector<vector<Value>> values {DuckDBPyConnection::transform_python_param_list(params)};
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
	}

	// cursor() is stupid
	unique_ptr<DuckDBPyConnection> cursor() {
		auto res = make_unique<DuckDBPyConnection>();
		res->database = database;
		res->connection = make_unique<Connection>(*res->database);
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

	static unique_ptr<DuckDBPyConnection> connect(string database, bool read_only) {
		auto res = make_unique<DuckDBPyConnection>();
		DBConfig config;
		if (read_only)
			config.access_mode = AccessMode::READ_ONLY;
		res->database = make_unique<DuckDB>(database, &config);
		res->connection = make_unique<Connection>(*res->database);

		PandasScanFunction scan_fun;
		CreateTableFunctionInfo info(scan_fun);

		auto &context = *res->connection->context;
		context.transaction.BeginTransaction();
		context.catalog.CreateTableFunction(context, &info);
		context.transaction.Commit();

		if (!read_only) {
			res->connection->Query("CREATE OR REPLACE VIEW sqlite_master AS SELECT * FROM sqlite_master()");
		}

		return res;
	}

	shared_ptr<DuckDB> database;
	unique_ptr<Connection> connection;
	unordered_map<string, py::object> registered_dfs;
	unique_ptr<DuckDBPyResult> result;

	static vector<Value> transform_python_param_list(py::handle params) {
		vector<Value> args;

		auto datetime_mod = py::module::import("datetime");
		auto datetime_date = datetime_mod.attr("datetime");
		auto datetime_datetime = datetime_mod.attr("date");

		for (auto &ele : params) {
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
			} else if (ele.get_type().is(datetime_date)) {
				throw runtime_error("date parameters not supported yet :/");
				// args.push_back(Value::DATE(1984, 4, 24));
			} else if (ele.get_type().is(datetime_datetime)) {
				throw runtime_error("datetime parameters not supported yet :/");
				// args.push_back(Value::TIMESTAMP(1984, 4, 24, 14, 42, 0, 0));
			} else {
				throw runtime_error("unknown param type " + py::str(ele.get_type()).cast<string>());
			}
		}
		return args;
	}
};

static unique_ptr<DuckDBPyConnection> default_connection_ = nullptr;

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

	static unique_ptr<DuckDBPyRelation> from_csv_auto(string filename) {
		return default_connection()->from_csv_auto(filename);
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
		vector<vector<Value>> values {DuckDBPyConnection::transform_python_param_list(params)};
		rel->Insert(values);
	}

	void create(string table) {
		rel->Create(table);
	}

	string print() {
		rel->Print();
		rel->Limit(10)->Execute()->Print();
		return "";
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
				res.append(SQLTypeToString(col.type));
			}
			return move(res);
		}
		return py::none();
	}

	shared_ptr<Relation> rel;
};

PYBIND11_MODULE(duckdb, m) {
	m.def("connect", &DuckDBPyConnection::connect, "some doc string",
	      py::arg("database") = ":memory:", py::arg("read_only") = false);

	auto conn_class =
	    py::class_<DuckDBPyConnection>(m, "DuckDBPyConnection")
	        .def("cursor", &DuckDBPyConnection::cursor)
	        .def("begin", &DuckDBPyConnection::begin)
	        .def("table", &DuckDBPyConnection::table, "some doc string for table", py::arg("name"))
	        .def("values", &DuckDBPyConnection::values, "some doc string for table", py::arg("values"))
	        .def("view", &DuckDBPyConnection::view, "some doc string for view", py::arg("name"))
	        .def("table_function", &DuckDBPyConnection::table_function, "some doc string for table_function",
	             py::arg("name"), py::arg("parameters") = py::list())
	        .def("from_df", &DuckDBPyConnection::from_df, "some doc string for from_df", py::arg("value"))
	        .def("from_csv_auto", &DuckDBPyConnection::from_csv_auto, "some doc string for from_csv_auto",
	             py::arg("filename"))
	        .def("df", &DuckDBPyConnection::from_df, "some doc string for df", py::arg("value"))
	        .def("commit", &DuckDBPyConnection::commit)
	        .def("rollback", &DuckDBPyConnection::rollback)
	        .def("execute", &DuckDBPyConnection::execute, "some doc string for execute", py::arg("query"),
	             py::arg("parameters") = py::list(), py::arg("multiple_parameter_sets") = false)
	        .def("executemany", &DuckDBPyConnection::executemany, "some doc string for executemany", py::arg("query"),
	             py::arg("parameters") = py::list())
	        .def("append", &DuckDBPyConnection::append, py::arg("table"), py::arg("value"))
	        .def("register", &DuckDBPyConnection::register_df, py::arg("name"), py::arg("value"))
	        .def("unregister", &DuckDBPyConnection::unregister_df, py::arg("name"))
	        .def("close", &DuckDBPyConnection::close)
	        .def("fetchone", &DuckDBPyConnection::fetchone)
	        .def("fetchall", &DuckDBPyConnection::fetchall)
	        .def("fetchnumpy", &DuckDBPyConnection::fetchnumpy)
	        .def("fetchdf", &DuckDBPyConnection::fetchdf)
	        .def("__getattr__", &DuckDBPyConnection::getattr);

	py::class_<DuckDBPyResult>(m, "DuckDBPyResult")
	    .def("close", &DuckDBPyResult::close)
	    .def("fetchone", &DuckDBPyResult::fetchone)
	    .def("fetchall", &DuckDBPyResult::fetchall)
	    .def("fetchnumpy", &DuckDBPyResult::fetchnumpy)
	    .def("fetchdf", &DuckDBPyResult::fetchdf)
	    .def("fetch_df", &DuckDBPyResult::fetchdf)
	    .def("df", &DuckDBPyResult::fetchdf);

	py::class_<DuckDBPyRelation>(m, "DuckDBPyRelation")
	    .def("filter", &DuckDBPyRelation::filter, "some doc string for filter", py::arg("filter_expr"))
	    .def("project", &DuckDBPyRelation::project, "some doc string for project", py::arg("project_expr"))
	    .def("set_alias", &DuckDBPyRelation::alias, "some doc string for alias", py::arg("alias"))
	    .def("order", &DuckDBPyRelation::order, "some doc string for order", py::arg("order_expr"))
	    .def("aggregate", &DuckDBPyRelation::aggregate, "some doc string for aggregate", py::arg("aggr_expr"),
	         py::arg("group_expr") = "")
	    .def("union", &DuckDBPyRelation::union_, "some doc string for union")
	    .def("except_", &DuckDBPyRelation::except, "some doc string for except", py::arg("other_rel"))
	    .def("intersect", &DuckDBPyRelation::intersect, "some doc string for intersect", py::arg("other_rel"))
	    .def("join", &DuckDBPyRelation::join, "some doc string for join", py::arg("other_rel"),
	         py::arg("join_condition"))
	    .def("distinct", &DuckDBPyRelation::distinct, "some doc string for distinct")
	    .def("limit", &DuckDBPyRelation::limit, "some doc string for limit", py::arg("n"))
	    .def("query", &DuckDBPyRelation::query, "some doc string for query", py::arg("virtual_table_name"),
	         py::arg("sql_query"))
	    .def("execute", &DuckDBPyRelation::execute, "some doc string for execute")
	    .def("write_csv", &DuckDBPyRelation::write_csv, "some doc string for write_csv", py::arg("file_name"))
	    .def("insert_into", &DuckDBPyRelation::insert_into, "some doc string for insert_into", py::arg("table_name"))
	    .def("insert", &DuckDBPyRelation::insert, "some doc string for insert", py::arg("values"))
	    .def("create", &DuckDBPyRelation::create, "some doc string for create", py::arg("table_name"))
	    .def("to_df", &DuckDBPyRelation::to_df, "some doc string for to_df")
	    .def("create_view", &DuckDBPyRelation::create_view, "some doc string for create_view", py::arg("view_name"),
	         py::arg("replace") = true)
	    .def("df", &DuckDBPyRelation::to_df, "some doc string for df")
	    .def("__str__", &DuckDBPyRelation::print, "some doc string for print")
	    .def("__repr__", &DuckDBPyRelation::print, "some doc string for repr")
	    .def("__getattr__", &DuckDBPyRelation::getattr);

	m.def("from_df", &DuckDBPyRelation::from_df, "some doc string for filter", py::arg("df"));
	m.def("from_csv_auto", &DuckDBPyRelation::from_csv_auto, "some doc string for from_csv_auto", py::arg("filename"));
	m.def("df", &DuckDBPyRelation::from_df, "some doc string for filter", py::arg("df"));
	m.def("filter", &DuckDBPyRelation::filter_df, "some doc string for filter", py::arg("df"), py::arg("filter_expr"));
	m.def("project", &DuckDBPyRelation::project_df, "some doc string for project", py::arg("df"),
	      py::arg("project_expr"));
	m.def("alias", &DuckDBPyRelation::alias_df, "some doc string for alias", py::arg("df"), py::arg("alias"));
	m.def("order", &DuckDBPyRelation::order_df, "some doc string for order", py::arg("df"), py::arg("order_expr"));
	m.def("aggregate", &DuckDBPyRelation::aggregate_df, "some doc string for aggregate", py::arg("df"),
	      py::arg("aggr_expr"), py::arg("group_expr") = "");
	m.def("distinct", &DuckDBPyRelation::distinct_df, "some doc string for distinct", py::arg("df"));
	m.def("limit", &DuckDBPyRelation::limit_df, "some doc string for limit", py::arg("df"), py::arg("n"));
	m.def("query", &DuckDBPyRelation::query_df, "some doc string for query", py::arg("df"),
	      py::arg("virtual_table_name"), py::arg("sql_query"));
	m.def("write_csv", &DuckDBPyRelation::write_csv_df, "some doc string for write_csv", py::arg("df"),
	      py::arg("file_name"));

	// we need this because otherwise we try to remove registered_dfs on shutdown when python is already dead
	auto clean_default_connection = []() { default_connection_ = nullptr; };
	m.add_object("_clean_default_connection", py::capsule(clean_default_connection));
	PyDateTime_IMPORT;
}
