#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <vector>
#include "datetime.h" // from Python

#include "duckdb.hpp"

namespace py = pybind11;

using namespace duckdb;
using namespace std;

struct DuckDBPyResult {

	template<class SRC>
	static SRC fetch_scalar(Vector &src_vec, idx_t offset) {
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
						Date::ExtractYear(date), Date::ExtractMonth(date),
						Date::ExtractDay(date), Timestamp::GetHours(timestamp),
						Timestamp::GetMinutes(timestamp),
						Timestamp::GetSeconds(timestamp),
						Timestamp::GetMilliseconds(timestamp) * 1000
								- Timestamp::GetSeconds(timestamp) * 1000000);

				break;
			}

			case SQLTypeId::DATE: {
				if (result->types[col_idx] != TypeId::INT32) {
					throw runtime_error("expected int32 for date");
				}
				auto date = val.GetValue<int32_t>();
				res[col_idx] = PyDate_FromDate(duckdb::Date::ExtractYear(date),
						duckdb::Date::ExtractMonth(date),
						duckdb::Date::ExtractDay(date));
				break;
			}

			default:
				throw runtime_error(
						"unsupported type: "
								+ SQLTypeToString(result->sql_types[col_idx]));
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

	static py::array fetch_string_column(ChunkCollection &collection,
			idx_t column) {
		// we cannot directly create an object numpy array
		auto out_l = py::list(collection.count);

		idx_t out_offset = 0;
		for (auto &data_chunk : collection.chunks) {
			auto &src = data_chunk->data[column];
			auto src_ptr = FlatVector::GetData<string_t>(src);
			auto &nullmask = FlatVector::Nullmask(src);

			for (idx_t i = 0; i < data_chunk->size(); i++) {
				if (nullmask[i]) {
					continue;
				}
				out_l[i + out_offset] = py::str(src_ptr[i].GetData());
			}
			out_offset += data_chunk->size();
		}
		return py::array(out_l);
	}

	template<class T>
	static py::array fetch_column(ChunkCollection &collection, idx_t column) {
		py::array_t<T> out;
		out.resize( { collection.count });
		T *out_ptr = out.mutable_data();

		idx_t out_offset = 0;
		for (auto &data_chunk : collection.chunks) {
			auto src_ptr = FlatVector::GetData<T>(data_chunk->data[column]);
			for (idx_t i = 0; i < data_chunk->size(); i++) {
				// never mind the nullmask here, will be faster
				out_ptr[i + out_offset] = (T) src_ptr[i];
			}
			out_offset += data_chunk->size();
		}
		return move(out);
	}

	py::dict fetchnumpy() {
		if (!result) {
			throw runtime_error("result closed");
		}
		// need to materialize the result if it was streamed because we need the count :/
		MaterializedQueryResult *mres = nullptr;
		unique_ptr<QueryResult> mat_res_holder;
		if (result->type == QueryResultType::STREAM_RESULT) {
			mat_res_holder = ((StreamQueryResult*) result.get())->Materialize();
			mres = (MaterializedQueryResult*) mat_res_holder.get();
		} else {
			mres = (MaterializedQueryResult*) result.get();
		}
		assert(mres);

		py::dict res;
		for (idx_t col_idx = 0; col_idx < mres->types.size(); col_idx++) {
			// convert the actual payload
			py::array col_res;
			switch (mres->sql_types[col_idx].id) {
			case SQLTypeId::BOOLEAN:
				col_res = fetch_column<bool>(mres->collection, col_idx);
				break;
			case SQLTypeId::TINYINT:
				col_res = fetch_column<int8_t>(mres->collection, col_idx);
				break;
			case SQLTypeId::SMALLINT:
				col_res = fetch_column<int16_t>(mres->collection, col_idx);
				break;
			case SQLTypeId::INTEGER:
				col_res = fetch_column<int32_t>(mres->collection, col_idx);
				break;
			case SQLTypeId::BIGINT:
				col_res = fetch_column<int64_t>(mres->collection, col_idx);
				break;
			case SQLTypeId::FLOAT:
				col_res = fetch_column<float>(mres->collection, col_idx);
				break;
			case SQLTypeId::DOUBLE:
				col_res = fetch_column<double>(mres->collection, col_idx);
				break;
			case SQLTypeId::VARCHAR:
				col_res = fetch_string_column(mres->collection, col_idx);
				break;
			default:
				throw runtime_error(
						"unsupported type "
								+ SQLTypeToString(mres->sql_types[col_idx]));
			}

			// convert the nullmask
			py::array_t<bool> nullmask;
			nullmask.resize( { mres->collection.count });
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
			auto masked_array = py::module::import("numpy.ma").attr(
					"masked_array")(col_res, nullmask);
			res[mres->names[col_idx].c_str()] = masked_array;
		}
		return res;
	}

	py::object fetchdf() {
		return py::module::import("pandas").attr("DataFrame").attr("from_dict")(
				fetchnumpy());
	}

	void close() {
		result = nullptr;
	}
	idx_t chunk_offset = 0;

	unique_ptr<QueryResult> result;
	unique_ptr<DataChunk> current_chunk;

};

//PyObject *duckdb_cursor_getiter(duckdb_Cursor *self);
//PyObject *duckdb_cursor_iternext(duckdb_Cursor *self);
//PyObject *duckdb_cursor_fetchdf(duckdb_Cursor *self);//

struct DuckDBPyConnection {
	// TODO parameters
	DuckDBPyConnection* execute(string query) {
		if (!connection) {
			throw runtime_error("connection closed");
		}
		result = nullptr;
		auto res = make_unique<DuckDBPyResult>();
		res->result = connection->Query(query);
		if (!res->result->success) {
			throw runtime_error(res->result->error);
		}
		result = move(res);
		return this;
	}

	DuckDBPyConnection* append(string name, py::object value) {
		register_df("__append_df", value);
		return execute("INSERT INTO \"" + name + "\" SELECT * FROM __append_df");
	}

	DuckDBPyConnection* register_df(string name, py::object value) {
		std::ostringstream address;
		address << (void const*) value.ptr();
		string pointer_str = address.str();

		// hack alert: put the pointer address into the function call as a string
		execute(
				"CREATE OR REPLACE TEMPORARY VIEW \"" + name
						+ "\" AS SELECT * FROM pandas_scan('" + pointer_str
						+ "')");

		// keep a reference
		registered_dfs.push_back(value);

		return this;
	}

	DuckDBPyConnection* begin() {
		execute("BEGIN TRANSACTION");
		return this;
	}

	DuckDBPyConnection* commit() {
		execute("COMMIT");
		return this;
	}

	DuckDBPyConnection* rollback() {
		execute("ROLLBACK");
		return this;
	}

	void close() {
		connection = nullptr;
		database = nullptr;
	}

	// cursor() is stupid
	DuckDBPyConnection* cursor() {
		return this;
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

	unique_ptr<DuckDB> database;
	unique_ptr<Connection> connection;
	vector<py::object> registered_dfs;
	unique_ptr<DuckDBPyResult> result;
};

struct PandasScanFunctionData: public TableFunctionData {
	PandasScanFunctionData(py::handle df, idx_t row_count) :
			df(df), row_count(row_count), position(0) {
	}
	py::handle df;
	idx_t row_count;
	idx_t position;

};

struct PandasScanFunction: public TableFunction {
	PandasScanFunction() :
			TableFunction("pandas_scan", { SQLType::VARCHAR }, pandas_scan_bind,
					pandas_scan_function, nullptr) {
	}
	;

	static unique_ptr<FunctionData> pandas_scan_bind(ClientContext &context,
			vector<Value> inputs, vector<SQLType> &return_types,
			vector<string> &names) {
		// Hey, it works (TM)
		py::handle df(
				(PyObject*) stoul(inputs[0].GetValue<string>(), nullptr, 16));

		auto df_names = py::list(df.attr("columns"));
		auto df_types = py::list(df.attr("dtypes"));

		// TODO support dicts of numpy arrays as well
		if (py::len(df_names) == 0) {
			throw runtime_error("need a dataframe with at least one column");
		}
		for (idx_t col_idx = 0; col_idx < py::len(df_names); col_idx++) {
			auto col_type = string(py::str(df_types[col_idx]));
			names.push_back(string(py::str(df_names[col_idx])));
			SQLType duckdb_col_type;
			if (col_type == "int64") {
				duckdb_col_type = SQLType::BIGINT;
			} else if (col_type == "object") {
				// this better be strings
				duckdb_col_type = SQLType::VARCHAR;
			} else {
				throw runtime_error("unsupported python type " + col_type);
			}
			return_types.push_back(duckdb_col_type);
		}
		idx_t row_count = py::len(df.attr("__getitem__")(df_names[0]));
		return make_unique<PandasScanFunctionData>(df, row_count);
	}

	template<class T>
	static void scan_pandas_column(py::array numpy_col, idx_t count,
			idx_t offset, Vector &out) {
		auto src_ptr = (T*) numpy_col.data();
		auto tgt_ptr = FlatVector::GetData<T>(out);

		for (idx_t row = 0; row < count; row++) {
			tgt_ptr[row] = src_ptr[row + offset];
		}
	}

	static void pandas_scan_function(ClientContext &context,
			vector<Value> &input, DataChunk &output, FunctionData *dataptr) {
		auto &data = *((PandasScanFunctionData*) dataptr);

		if (data.position >= data.row_count) {
			return;
		}
		idx_t this_count = std::min((idx_t) STANDARD_VECTOR_SIZE,
				data.row_count - data.position);

		auto df_names = py::list(data.df.attr("columns"));
		auto get_fun = data.df.attr("__getitem__");

		output.SetCardinality(this_count);
		for (idx_t col_idx = 0; col_idx < output.column_count(); col_idx++) {
			auto numpy_col = py::array(
					get_fun(df_names[col_idx]).attr("to_numpy")());
			switch (output.GetTypes()[col_idx]) {
			case TypeId::BOOL:
				scan_pandas_column<bool>(numpy_col, this_count, data.position,
						output.data[col_idx]);
				break;
			case TypeId::INT8:
				scan_pandas_column<int8_t>(numpy_col, this_count, data.position,
						output.data[col_idx]);
				break;
			case TypeId::INT16:
				scan_pandas_column<int16_t>(numpy_col, this_count,
						data.position, output.data[col_idx]);
				break;
			case TypeId::INT32:
				scan_pandas_column<int32_t>(numpy_col, this_count,
						data.position, output.data[col_idx]);
				break;
			case TypeId::INT64:
				scan_pandas_column<int64_t>(numpy_col, this_count,
						data.position, output.data[col_idx]);
				break;
			case TypeId::FLOAT:
				scan_pandas_column<float>(numpy_col, this_count, data.position,
						output.data[col_idx]);
				break;
			case TypeId::DOUBLE:
				scan_pandas_column<double>(numpy_col, this_count, data.position,
						output.data[col_idx]);
				break;
			case TypeId::VARCHAR: {
				auto src_ptr = (py::object*) numpy_col.data();
				auto tgt_ptr = (string_t*) FlatVector::GetData(
						output.data[col_idx]);

				for (idx_t row = 0; row < this_count; row++) {
					tgt_ptr[row] = StringVector::AddString(output.data[col_idx],
							src_ptr[row + data.position].cast<string>());
				}
				break;
			}
			default:
				throw runtime_error("unsupported type"); // FIXME make this more informative
			}
		}
		data.position += this_count;
	}

};

static unique_ptr<DuckDBPyConnection> connect(string database, bool read_only) {
	auto res = make_unique<DuckDBPyConnection>();
	res->database = make_unique<DuckDB>(database);
	res->connection = make_unique<Connection>(*res->database);

	PandasScanFunction scan_fun;
	CreateTableFunctionInfo info(scan_fun);
	auto &context = *res->connection->context;
	context.transaction.BeginTransaction();
	context.catalog.CreateTableFunction(context, &info);
	context.transaction.Commit();
	return res;
}

PYBIND11_MODULE(duckdb, m) {
	m.def("connect", &connect, "some doc string", py::arg("database") = ":memory:", py::arg("read_only") = false );

	py::class_<DuckDBPyConnection>(m, "DuckDBPyConnection")
	.def("cursor", &DuckDBPyConnection::cursor, "some doc string")
	.def("begin", &DuckDBPyConnection::begin)
	.def("commit", &DuckDBPyConnection::commit)
	.def("rollback", &DuckDBPyConnection::rollback)
	.def("execute", &DuckDBPyConnection::execute)
	.def("append", &DuckDBPyConnection::append)
	.def("register", &DuckDBPyConnection::register_df)
	.def("close", &DuckDBPyConnection::close)
	.def("fetchone", &DuckDBPyConnection::fetchone)
	.def("fetchall", &DuckDBPyConnection::fetchall)
	.def("fetchnumpy", &DuckDBPyConnection::fetchnumpy)
	.def("fetchdf", &DuckDBPyConnection::fetchdf);

	PyDateTime_IMPORT;

}
