#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <vector>

#include "duckdb.hpp"

namespace py = pybind11;

using namespace duckdb;
using namespace std;

template<class SRC>
static SRC fetch_scalar(Vector &src_vec, idx_t offset) {
	auto src_ptr = FlatVector::GetData<SRC>(src_vec);
	return src_ptr[offset];
}

struct DuckDBPyResult {
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

		for (idx_t i = 0; i < result->types.size(); i++) {
			auto &nullmask = FlatVector::Nullmask(current_chunk->data[i]);
			if (nullmask[chunk_offset]) {
				res[i] = py::none();
				continue;
			}
			// TODO other types
			switch (result->types[i]) {
			case TypeId::BOOL:
				res[i] = fetch_scalar<bool>(current_chunk->data[i],
						chunk_offset);
				break;
			case TypeId::INT8:
				res[i] = fetch_scalar<int8_t>(current_chunk->data[i],
						chunk_offset);
				break;
			case TypeId::INT16:
				res[i] = fetch_scalar<int16_t>(current_chunk->data[i],
						chunk_offset);
				break;
			case TypeId::INT32:
				res[i] = fetch_scalar<int32_t>(current_chunk->data[i],
						chunk_offset);
				break;
			case TypeId::INT64:
				res[i] = fetch_scalar<int64_t>(current_chunk->data[i],
						chunk_offset);
				break;
			case TypeId::FLOAT:
				res[i] = fetch_scalar<float>(current_chunk->data[i],
						chunk_offset);
				break;
			case TypeId::DOUBLE:
				res[i] = fetch_scalar<double>(current_chunk->data[i],
						chunk_offset);
				break;
			case TypeId::VARCHAR:
				res[i] = fetch_scalar<string_t>(current_chunk->data[i],
						chunk_offset).GetData();
				break;
			default:
				throw runtime_error("unsupported type");
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
			switch (mres->types[col_idx]) {
			case TypeId::BOOL:
				col_res = fetch_column<bool>(mres->collection, col_idx);
				break;
			case TypeId::INT8:
				col_res = fetch_column<int8_t>(mres->collection, col_idx);
				break;
			case TypeId::INT16:
				col_res = fetch_column<int16_t>(mres->collection, col_idx);
				break;
			case TypeId::INT32:
				col_res = fetch_column<int32_t>(mres->collection, col_idx);
				break;
			case TypeId::INT64:
				col_res = fetch_column<int64_t>(mres->collection, col_idx);
				break;
			case TypeId::FLOAT:
				col_res = fetch_column<float>(mres->collection, col_idx);
				break;
			case TypeId::DOUBLE:
				col_res = fetch_column<double>(mres->collection, col_idx);
				break;
			case TypeId::VARCHAR:
				col_res = fetch_string_column(mres->collection, col_idx);
				break;

			default:
				throw runtime_error("unsupported type");
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
	unique_ptr<DuckDBPyResult> execute(string query) {
		if (!connection) {
			throw runtime_error("connection closed");
		}
		auto res = make_unique<DuckDBPyResult>();
		res->result = connection->Query(query);
		if (!res->result->success) {
			throw runtime_error(res->result->error);
		}
		return res;
	}

	unique_ptr<DuckDBPyResult> append(string name, py::object value) {
		register_df("__append_df", value);
		return execute("INSERT INTO \"" + name + "\" SELECT * FROM __append_df");
	}

	DuckDBPyConnection* register_df(string name, py::object value) {
		if (!connection) {
			throw runtime_error("connection closed");
		}

		// keep a reference
		registered_dfs.push_back(value);

		std::ostringstream address;
		address << (void const*) value.ptr();
		string pointer_str = address.str();

		// hack alert: put the pointer address into the function call
		execute(
				"CREATE OR REPLACE TEMPORARY VIEW \"" + name
						+ "\" AS SELECT * FROM pandas_scan('" + pointer_str
						+ "')");
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

	unique_ptr<DuckDB> database;
	unique_ptr<Connection> connection;
	vector<py::object> registered_dfs;
};

struct PandasScanFunctionData: public TableFunctionData {
	PandasScanFunctionData(py::handle df, idx_t row_count) :
			df(df), row_count(row_count), position(0) {
	}
	py::handle df;
	idx_t row_count;
	idx_t position;

};

static unique_ptr<FunctionData> pandas_scan_bind(ClientContext &context,
		vector<Value> inputs, vector<SQLType> &return_types,
		vector<string> &names) {
	// Hey, it works (TM)
	py::handle df((PyObject*) stoul(inputs[0].GetValue<string>(), nullptr, 16));

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

void pandas_scan_function(ClientContext &context, vector<Value> &input,
		DataChunk &output, FunctionData *dataptr) {
	auto &data = *((PandasScanFunctionData*) dataptr);

	if (data.position >= data.row_count) {
		return;
	}

	auto df_names = py::list(data.df.attr("columns"));
	auto get_fun = data.df.attr("__getitem__");

	output.SetCardinality(data.row_count);
	for (idx_t col_idx = 0; col_idx < output.column_count(); col_idx++) {
		auto numpy_col = py::array(
				get_fun(df_names[col_idx]).attr("to_numpy")());
		switch (output.GetTypes()[col_idx]) {
		case TypeId::INT64: {
			auto src_ptr = (int64_t*) numpy_col.data();
			auto tgt_ptr = (int64_t*) FlatVector::GetData(output.data[col_idx]);

			for (idx_t row = 0; row < data.row_count; row++) {
				tgt_ptr[row] = src_ptr[row];
			}
			break;
		}
		case TypeId::VARCHAR: {
			auto src_ptr = (py::object*) numpy_col.data();
			auto tgt_ptr = (string_t*) FlatVector::GetData(
					output.data[col_idx]);

			for (idx_t row = 0; row < data.row_count; row++) {
				tgt_ptr[row] = StringVector::AddString(output.data[col_idx],
						src_ptr[row].cast<string>());
			}
			break;
		}
		default:
			throw runtime_error("unsupported type"); // FIXME make this more informative
		}
	}
	data.position = data.row_count;
}

class PandasScanFunction: public TableFunction {
public:
	PandasScanFunction() :
			TableFunction("pandas_scan", { SQLType::VARCHAR }, pandas_scan_bind,
					pandas_scan_function, nullptr) {
	}
	;
};

unique_ptr<DuckDBPyConnection> connect(string database, bool read_only) {
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
	.def("close", &DuckDBPyConnection::close);

	py::class_<DuckDBPyResult>(m, "DuckDBPyResult")
	.def("fetchone", &DuckDBPyResult::fetchone)
	.def("fetchall", &DuckDBPyResult::fetchall)
	.def("fetchnumpy", &DuckDBPyResult::fetchnumpy)
	.def("fetchdf", &DuckDBPyResult::fetchdf)
	.def("close", &DuckDBPyResult::close);
}
