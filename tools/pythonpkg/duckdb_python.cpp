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

struct DuckDBPyRelation {

	DuckDBPyRelation(shared_ptr<Relation> rel) : rel(rel) {
	}

	unique_ptr<DuckDBPyRelation> project(string expr) {
		return make_unique<DuckDBPyRelation>(rel->Project(expr));
	}

	unique_ptr<DuckDBPyRelation> filter(string expr) {
		return make_unique<DuckDBPyRelation>(rel->Filter(expr));
	}

	unique_ptr<DuckDBPyRelation> limit(int64_t n) {
		return make_unique<DuckDBPyRelation>(rel->Limit(n));
	}

	unique_ptr<DuckDBPyRelation> order(string expr) {
		return make_unique<DuckDBPyRelation>(rel->Order(expr));
	}

	unique_ptr<DuckDBPyRelation> aggregate(string expr, string groups = "") {
		if (groups.size() > 0) {
			return make_unique<DuckDBPyRelation>(rel->Aggregate(expr, groups));
		}
		return make_unique<DuckDBPyRelation>(rel->Aggregate(expr));
	}

	unique_ptr<DuckDBPyRelation> distinct() {
		return make_unique<DuckDBPyRelation>(rel->Distinct());
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

	void insert(string table) {
		rel->Insert(table);
	}

	void create(string table) {
		rel->Create(table);
	}

	string print() {
		rel->Print();
		rel->Limit(10)->Execute()->Print();
		return "";
	}

	shared_ptr<Relation> rel;
};

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

struct DuckDBPyConnection {
	DuckDBPyConnection *executemany(string query, py::object params = py::list()) {
		execute(query, params, true);
		return this;
	}

	~DuckDBPyConnection() {
		for (auto& element : registered_dfs) {
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
			auto args = transform_python_param_list(single_query_params);
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

		return make_unique<DuckDBPyRelation>(connection->TableFunction(fname, transform_python_param_list(params)));
	}

	unique_ptr<DuckDBPyRelation> from_df(py::object value) {
		if (!connection) {
			throw runtime_error("connection closed");
		};
		registered_dfs[random_string::generate()] = value;
		vector<Value> params;
		params.push_back(Value(ptr_to_string(value.ptr())));
		return make_unique<DuckDBPyRelation>(connection->TableFunction("pandas_scan", params));
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

	shared_ptr<DuckDB> database;
	unique_ptr<Connection> connection;
	unordered_map<string, py::object> registered_dfs;
	unique_ptr<DuckDBPyResult> result;
};

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
		py::handle df((PyObject *)stoul(inputs[0].GetValue<string>(), nullptr, 16));

		auto df_names = py::list(df.attr("columns"));
		auto df_types = py::list(df.attr("dtypes"));
		// TODO support masked arrays as well
		// TODO support dicts of numpy arrays as well
		if (py::len(df_names) == 0) {
			throw runtime_error("need a dataframe with at least one column");
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
		auto tgt_ptr = FlatVector::GetData<T>(out);

		for (idx_t row = 0; row < count; row++) {
			tgt_ptr[row] = src_ptr[row + offset];
		}
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
					;
				}
				break;
			} break;
			case SQLTypeId::VARCHAR: {
				auto src_ptr = (py::object *)numpy_col.data();
				auto tgt_ptr = (string_t *)FlatVector::GetData(output.data[col_idx]);

				for (idx_t row = 0; row < this_count; row++) {
					auto val = src_ptr[row + data.position];

					if (!py::isinstance<py::str>(val)) {
						FlatVector::SetNull(output.data[col_idx], row, true);
						continue;
					}
					tgt_ptr[row] = StringVector::AddString(output.data[col_idx], val.cast<string>());
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

PYBIND11_MODULE(duckdb, m) {
	m.def("connect", &connect, "some doc string", py::arg("database") = ":memory:", py::arg("read_only") = false);

	py::class_<DuckDBPyConnection>(m, "DuckDBPyConnection")
	    .def("cursor", &DuckDBPyConnection::cursor)
	    .def("begin", &DuckDBPyConnection::begin)
	    .def("table", &DuckDBPyConnection::table, "some doc string for table", py::arg("name"))
	    .def("view", &DuckDBPyConnection::view, "some doc string for view", py::arg("name"))
	    .def("table_function", &DuckDBPyConnection::table_function, "some doc string for table_function",
	         py::arg("name"), py::arg("parameters") = py::list())
	    .def("from_df", &DuckDBPyConnection::from_df, "some doc string for from_df", py::arg("value"))
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

	py::class_<DuckDBPyRelation>(m, "DuckDBPyRelation")
	    .def("filter", &DuckDBPyRelation::filter, "some doc string for filter", py::arg("filter_expr"))
	    .def("project", &DuckDBPyRelation::project, "some doc string for project", py::arg("project_expr"))
	    .def("order", &DuckDBPyRelation::order, "some doc string for order", py::arg("order_expr"))
	    .def("aggregate", &DuckDBPyRelation::aggregate, "some doc string for aggregate", py::arg("aggr_expr"), py::arg("group_expr") = "")
	    .def("union", &DuckDBPyRelation::union_, "some doc string for union", py::arg("other_rel"))
	    .def("except", &DuckDBPyRelation::except, "some doc string for except", py::arg("other_rel"))
	    .def("intersect", &DuckDBPyRelation::intersect, "some doc string for intersect", py::arg("other_rel"))
	    .def("join", &DuckDBPyRelation::join, "some doc string for join", py::arg("other_rel"),
	         py::arg("join_condition"))
	    .def("distinct", &DuckDBPyRelation::distinct, "some doc string for distinct")
	    .def("limit", &DuckDBPyRelation::limit, "some doc string for limit", py::arg("n"))
	    .def("write_csv", &DuckDBPyRelation::write_csv, "some doc string for write_csv", py::arg("file_name"))
	    .def("insert", &DuckDBPyRelation::insert, "some doc string for insert", py::arg("table_name"))
	    .def("create", &DuckDBPyRelation::create, "some doc string for create", py::arg("table_name"))
	    .def("to_df", &DuckDBPyRelation::to_df, "some doc string for to_df")
	    .def("__str__", &DuckDBPyRelation::print);

	PyDateTime_IMPORT;
}
