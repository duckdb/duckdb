#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <unordered_map>
#include <vector>
#include <atomic>

#include "datetime.h" // from Python

#include "duckdb_python/array_wrapper.hpp"
#include "duckdb_python/pandas_scan.hpp"

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
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/printer.hpp"
#include <random>
#include <stdlib.h>

namespace py = pybind11;

namespace duckdb {

namespace random_string {
static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_int_distribution<> dis(0, 15);

std::string Generate() {
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
public:
	idx_t chunk_offset = 0;

	unique_ptr<QueryResult> result;
	unique_ptr<DataChunk> current_chunk;

public:
	template <class SRC>
	static SRC FetchScalar(Vector &src_vec, idx_t offset) {
		auto src_ptr = FlatVector::GetData<SRC>(src_vec);
		return src_ptr[offset];
	}

	py::object Fetchone() {
		if (!result) {
			throw std::runtime_error("result closed");
		}
		if (!current_chunk || chunk_offset >= current_chunk->size()) {
			current_chunk = result->Fetch();
			chunk_offset = 0;
		}
		if (!current_chunk || current_chunk->size() == 0) {
			return py::none();
		}
		py::tuple res(result->types.size());

		for (idx_t col_idx = 0; col_idx < result->types.size(); col_idx++) {
			auto &mask = FlatVector::Validity(current_chunk->data[col_idx]);
			if (!mask.RowIsValid(chunk_offset)) {
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
			case LogicalTypeId::UTINYINT:
				res[col_idx] = val.GetValue<uint8_t>();
				break;
			case LogicalTypeId::USMALLINT:
				res[col_idx] = val.GetValue<uint16_t>();
				break;
			case LogicalTypeId::UINTEGER:
				res[col_idx] = val.GetValue<uint32_t>();
				break;
			case LogicalTypeId::UBIGINT:
				res[col_idx] = val.GetValue<uint64_t>();
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
				throw std::runtime_error("unsupported type: " + result->types[col_idx].ToString());
			}
		}
		chunk_offset++;
		return move(res);
	}

	py::list Fetchall() {
		py::list res;
		while (true) {
			auto fres = Fetchone();
			if (fres.is_none()) {
				break;
			}
			res.append(fres);
		}
		return res;
	}

	py::dict FetchNumpy(bool stream = false) {
		if (!result) {
			throw std::runtime_error("result closed");
		}

		// iterate over the result to materialize the data needed for the NumPy arrays
		idx_t initial_capacity = STANDARD_VECTOR_SIZE * 2;
		if (result->type == QueryResultType::MATERIALIZED_RESULT) {
			// materialized query result: we know exactly how much space we need
			auto &materialized = (MaterializedQueryResult &)*result;
			initial_capacity = materialized.collection.Count();
		}

		NumpyResultConversion conversion(result->types, initial_capacity);
		if (result->type == QueryResultType::MATERIALIZED_RESULT) {
			auto &materialized = (MaterializedQueryResult &)*result;
			if (!stream) {
				for (auto &chunk : materialized.collection.Chunks()) {
					conversion.Append(*chunk);
				}
				materialized.collection.Reset();
			} else {
				conversion.Append(*materialized.Fetch());
			}
		} else {
			if (!stream) {
				while (true) {
					auto chunk = result->FetchRaw();
					if (!chunk || chunk->size() == 0) {
						// finished
						break;
					}
					conversion.Append(*chunk);
				}
			} else {
				auto chunk = result->FetchRaw();
				if (chunk && chunk->size() > 0) {
					conversion.Append(*chunk);
				}
			}
		}

		// now that we have materialized the result in contiguous arrays, construct the actual NumPy arrays
		py::dict res;
		for (idx_t col_idx = 0; col_idx < result->types.size(); col_idx++) {
			res[result->names[col_idx].c_str()] = conversion.ToArray(col_idx);
		}
		return res;
	}

	py::object FetchDF() {
		return py::module::import("pandas").attr("DataFrame").attr("from_dict")(FetchNumpy());
	}

	py::object FetchDFChunk() {
		return py::module::import("pandas").attr("DataFrame").attr("from_dict")(FetchNumpy(true));
	}

	py::object FetchArrowTable() {
		if (!result) {
			throw std::runtime_error("result closed");
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
			if (!data_chunk || data_chunk->size() == 0) {
				break;
			}
			ArrowArray data;
			data_chunk->ToArrowArray(&data);
			ArrowSchema arrow_schema;
			result->ToArrowSchema(&arrow_schema);
			batches.append(batch_import_func((uint64_t)&data, (uint64_t)&arrow_schema));
		}
		return from_batches_func(batches, schema_obj);
	}

	py::list Description() {
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

	void Close() {
		result = nullptr;
	}
};

struct DuckDBPyRelation;

struct DuckDBPyConnection {
	DuckDBPyConnection *ExecuteMany(const string &query, py::object params = py::list()) {
		Execute(query, std::move(params), true);
		return this;
	}

	~DuckDBPyConnection() {
		for (auto &element : registered_dfs) {
			UnregisterDF(element.first);
		}
	}

	DuckDBPyConnection *Execute(const string &query, py::object params = py::list(), bool many = false) {
		if (!connection) {
			throw std::runtime_error("connection closed");
		}
		result = nullptr;

		auto statements = connection->ExtractStatements(query);
		if (statements.empty()) {
			// no statements to execute
			return this;
		}
		// if there are multiple statements, we directly execute the statements besides the last one
		// we only return the result of the last statement to the user, unless one of the previous statements fails
		for (idx_t i = 0; i + 1 < statements.size(); i++) {
			auto res = connection->Query(move(statements[i]));
			if (!res->success) {
				throw std::runtime_error(res->error);
			}
		}

		auto prep = connection->Prepare(move(statements.back()));
		if (!prep->success) {
			throw std::runtime_error(prep->error);
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
				throw std::runtime_error("Prepared statement needs " + to_string(prep->n_param) + " parameters, " +
				                         to_string(py::len(single_query_params)) + " given");
			}
			auto args = DuckDBPyConnection::TransformPythonParamList(single_query_params);
			auto res = make_unique<DuckDBPyResult>();
			{
				py::gil_scoped_release release;
				res->result = prep->Execute(args);
			}
			if (!res->result->success) {
				throw std::runtime_error(res->result->error);
			}
			if (!many) {
				result = move(res);
			}
		}
		return this;
	}

	DuckDBPyConnection *Append(const string &name, py::object value) {
		RegisterDF("__append_df", std::move(value));
		return Execute("INSERT INTO \"" + name + "\" SELECT * FROM __append_df");
	}

	static string PtrToString(void const *ptr) {
		std::ostringstream address;
		address << ptr;
		return address.str();
	}

	DuckDBPyConnection *RegisterDF(const string &name, py::object value) {
		// hack alert: put the pointer address into the function call as a string
		Execute("CREATE OR REPLACE VIEW \"" + name + "\" AS SELECT * FROM pandas_scan('" + PtrToString(value.ptr()) +
		        "')");

		// try to bind
		Execute("SELECT * FROM \"" + name + "\" WHERE FALSE");

		// keep a reference
		registered_dfs[name] = value;
		return this;
	}

	unique_ptr<DuckDBPyRelation> Table(const string &tname) {
		if (!connection) {
			throw std::runtime_error("connection closed");
		}
		return make_unique<DuckDBPyRelation>(connection->Table(tname));
	}

	unique_ptr<DuckDBPyRelation> Values(py::object params = py::list()) {
		if (!connection) {
			throw std::runtime_error("connection closed");
		}
		vector<vector<Value>> values {DuckDBPyConnection::TransformPythonParamList(std::move(params))};
		return make_unique<DuckDBPyRelation>(connection->Values(values));
	}

	unique_ptr<DuckDBPyRelation> View(const string &vname) {
		if (!connection) {
			throw std::runtime_error("connection closed");
		}
		return make_unique<DuckDBPyRelation>(connection->View(vname));
	}

	unique_ptr<DuckDBPyRelation> TableFunction(const string &fname, py::object params = py::list()) {
		if (!connection) {
			throw std::runtime_error("connection closed");
		}

		return make_unique<DuckDBPyRelation>(
		    connection->TableFunction(fname, DuckDBPyConnection::TransformPythonParamList(std::move(params))));
	}

	unique_ptr<DuckDBPyRelation> FromDF(py::object value) {
		if (!connection) {
			throw std::runtime_error("connection closed");
		}
		string name = "df_" + random_string::Generate();
		registered_dfs[name] = value;
		vector<Value> params;
		params.emplace_back(PtrToString(value.ptr()));
		return make_unique<DuckDBPyRelation>(connection->TableFunction("pandas_scan", params)->Alias(name));
	}

	unique_ptr<DuckDBPyRelation> FromCsvAuto(const string &filename) {
		if (!connection) {
			throw std::runtime_error("connection closed");
		}
		vector<Value> params;
		params.emplace_back(filename);
		return make_unique<DuckDBPyRelation>(connection->TableFunction("read_csv_auto", params)->Alias(filename));
	}

	unique_ptr<DuckDBPyRelation> FromParquet(const string &filename) {
		if (!connection) {
			throw std::runtime_error("connection closed");
		}
		vector<Value> params;
		params.emplace_back(filename);
		return make_unique<DuckDBPyRelation>(connection->TableFunction("parquet_scan", params)->Alias(filename));
	}

	struct PythonTableArrowArrayStream {
		explicit PythonTableArrowArrayStream(const py::object &arrow_table) : arrow_table(arrow_table) {
			stream.get_schema = PythonTableArrowArrayStream::MyStreamGetSchema;
			stream.get_next = PythonTableArrowArrayStream::MyStreamGetNext;
			stream.release = PythonTableArrowArrayStream::MyStreamRelease;
			stream.get_last_error = PythonTableArrowArrayStream::MyStreamGetLastError;
			stream.private_data = this;

			batches = arrow_table.attr("to_batches")();
		}

		static int MyStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
			D_ASSERT(stream->private_data);
			auto my_stream = (PythonTableArrowArrayStream *)stream->private_data;
			if (!stream->release) {
				my_stream->last_error = "stream was released";
				return -1;
			}
			my_stream->arrow_table.attr("schema").attr("_export_to_c")((uint64_t)out);
			return 0;
		}

		static int MyStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
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

		static void MyStreamRelease(struct ArrowArrayStream *stream) {
			if (!stream->release) {
				return;
			}
			stream->release = nullptr;
			delete (PythonTableArrowArrayStream *)stream->private_data;
		}

		static const char *MyStreamGetLastError(struct ArrowArrayStream *stream) {
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

	unique_ptr<DuckDBPyRelation> FromArrowTable(const py::object &table) {
		if (!connection) {
			throw std::runtime_error("connection closed");
		}

		// the following is a careful dance around having to depend on pyarrow
		if (table.is_none() || string(py::str(table.get_type().attr("__name__"))) != "Table") {
			throw std::runtime_error("Only arrow tables supported");
		}

		auto my_arrow_table = new PythonTableArrowArrayStream(table);
		string name = "arrow_table_" + PtrToString((void *)my_arrow_table);
		return make_unique<DuckDBPyRelation>(
		    connection->TableFunction("arrow_scan", {Value::POINTER((uintptr_t)my_arrow_table)})->Alias(name));
	}

	DuckDBPyConnection *UnregisterDF(const string &name) {
		registered_dfs[name] = py::none();
		return this;
	}

	DuckDBPyConnection *Begin() {
		Execute("BEGIN TRANSACTION");
		return this;
	}

	DuckDBPyConnection *Commit() {
		if (connection->context->transaction.IsAutoCommit()) {
			return this;
		}
		Execute("COMMIT");
		return this;
	}

	DuckDBPyConnection *Rollback() {
		Execute("ROLLBACK");
		return this;
	}

	py::object GetAttr(const py::str &key) {
		if (key.cast<string>() == "description") {
			if (!result) {
				throw std::runtime_error("no open result set");
			}
			return result->Description();
		}
		return py::none();
	}

	void Close() {
		result = nullptr;
		connection = nullptr;
		database = nullptr;
		for (auto &cur : cursors) {
			cur->Close();
		}
		cursors.clear();
	}

	// cursor() is stupid
	shared_ptr<DuckDBPyConnection> Cursor() {
		auto res = make_shared<DuckDBPyConnection>();
		res->database = database;
		res->connection = make_unique<Connection>(*res->database);
		cursors.push_back(res);
		return res;
	}

	// these should be functions on the result but well
	py::object FetchOne() {
		if (!result) {
			throw std::runtime_error("no open result set");
		}
		return result->Fetchone();
	}

	py::list FetchAll() {
		if (!result) {
			throw std::runtime_error("no open result set");
		}
		return result->Fetchall();
	}

	py::dict FetchNumpy() {
		if (!result) {
			throw std::runtime_error("no open result set");
		}
		return result->FetchNumpy();
	}
	py::object FetchDF() {
		if (!result) {
			throw std::runtime_error("no open result set");
		}
		return result->FetchDF();
	}

	py::object FetchDFChunk() const {
		if (!result) {
			throw std::runtime_error("no open result set");
		}
		return result->FetchDFChunk();
	}

	py::object FetchArrow() {
		if (!result) {
			throw std::runtime_error("no open result set");
		}
		return result->FetchArrowTable();
	}

	static shared_ptr<DuckDBPyConnection> Connect(const string &database, bool read_only) {
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
		auto &catalog = Catalog::GetCatalog(context);
		context.transaction.BeginTransaction();
		catalog.CreateTableFunction(context, &info);
		context.transaction.Commit();

		return res;
	}

	shared_ptr<DuckDB> database;
	unique_ptr<Connection> connection;
	unordered_map<string, py::object> registered_dfs;
	unique_ptr<DuckDBPyResult> result;
	vector<shared_ptr<DuckDBPyConnection>> cursors;

	static vector<Value> TransformPythonParamList(py::handle params) {
		vector<Value> args;

		auto datetime_mod = py::module::import("datetime");
		auto datetime_date = datetime_mod.attr("date");
		auto datetime_datetime = datetime_mod.attr("datetime");
		auto datetime_time = datetime_mod.attr("time");
		auto decimal_mod = py::module::import("decimal");
		auto decimal_decimal = decimal_mod.attr("Decimal");

		for (pybind11::handle ele : params) {
			if (ele.is_none()) {
				args.emplace_back();
			} else if (py::isinstance<py::bool_>(ele)) {
				args.push_back(Value::BOOLEAN(ele.cast<bool>()));
			} else if (py::isinstance<py::int_>(ele)) {
				args.push_back(Value::BIGINT(ele.cast<int64_t>()));
			} else if (py::isinstance<py::float_>(ele)) {
				args.push_back(Value::DOUBLE(ele.cast<double>()));
			} else if (py::isinstance<py::str>(ele)) {
				args.emplace_back(ele.cast<string>());
			} else if (py::isinstance(ele, decimal_decimal)) {
				args.emplace_back(py::str(ele).cast<string>());
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
				throw std::runtime_error("unknown param type " + py::str(ele.get_type()).cast<string>());
			}
		}
		return args;
	}
};

static shared_ptr<DuckDBPyConnection> default_connection = nullptr;

static DuckDBPyConnection *DefaultConnection() {
	if (!default_connection) {
		default_connection = DuckDBPyConnection::Connect(":memory:", false);
	}
	return default_connection.get();
}

struct DuckDBPyRelation {

	explicit DuckDBPyRelation(shared_ptr<Relation> rel) : rel(std::move(rel)) {
	}

	static unique_ptr<DuckDBPyRelation> FromDf(py::object df) {
		return DefaultConnection()->FromDF(std::move(df));
	}

	static unique_ptr<DuckDBPyRelation> Values(py::object values = py::list()) {
		return DefaultConnection()->Values(std::move(values));
	}

	static unique_ptr<DuckDBPyRelation> FromCsvAuto(const string &filename) {
		return DefaultConnection()->FromCsvAuto(filename);
	}

	static unique_ptr<DuckDBPyRelation> FromParquet(const string &filename) {
		return DefaultConnection()->FromParquet(filename);
	}

	static unique_ptr<DuckDBPyRelation> FromArrowTable(const py::object &table) {
		return DefaultConnection()->FromArrowTable(table);
	}

	unique_ptr<DuckDBPyRelation> Project(const string &expr) {
		return make_unique<DuckDBPyRelation>(rel->Project(expr));
	}

	static unique_ptr<DuckDBPyRelation> ProjectDf(py::object df, const string &expr) {
		return DefaultConnection()->FromDF(std::move(df))->Project(expr);
	}

	unique_ptr<DuckDBPyRelation> Alias(const string &expr) {
		return make_unique<DuckDBPyRelation>(rel->Alias(expr));
	}

	static unique_ptr<DuckDBPyRelation> AliasDF(py::object df, const string &expr) {
		return DefaultConnection()->FromDF(std::move(df))->Alias(expr);
	}

	unique_ptr<DuckDBPyRelation> Filter(const string &expr) {
		return make_unique<DuckDBPyRelation>(rel->Filter(expr));
	}

	static unique_ptr<DuckDBPyRelation> FilterDf(py::object df, const string &expr) {
		return DefaultConnection()->FromDF(std::move(df))->Filter(expr);
	}

	unique_ptr<DuckDBPyRelation> Limit(int64_t n) {
		return make_unique<DuckDBPyRelation>(rel->Limit(n));
	}

	static unique_ptr<DuckDBPyRelation> LimitDF(py::object df, int64_t n) {
		return DefaultConnection()->FromDF(std::move(df))->Limit(n);
	}

	unique_ptr<DuckDBPyRelation> Order(const string &expr) {
		return make_unique<DuckDBPyRelation>(rel->Order(expr));
	}

	static unique_ptr<DuckDBPyRelation> OrderDf(py::object df, const string &expr) {
		return DefaultConnection()->FromDF(std::move(df))->Order(expr);
	}

	unique_ptr<DuckDBPyRelation> Aggregate(const string &expr, const string &groups = "") {
		if (!groups.empty()) {
			return make_unique<DuckDBPyRelation>(rel->Aggregate(expr, groups));
		}
		return make_unique<DuckDBPyRelation>(rel->Aggregate(expr));
	}

	static unique_ptr<DuckDBPyRelation> AggregateDF(py::object df, const string &expr, const string &groups = "") {
		return DefaultConnection()->FromDF(std::move(df))->Aggregate(expr, groups);
	}

	unique_ptr<DuckDBPyRelation> Distinct() {
		return make_unique<DuckDBPyRelation>(rel->Distinct());
	}

	static unique_ptr<DuckDBPyRelation> DistinctDF(py::object df) {
		return DefaultConnection()->FromDF(std::move(df))->Distinct();
	}

	py::object ToDF() {
		auto res = make_unique<DuckDBPyResult>();
		{
			py::gil_scoped_release release;
			res->result = rel->Execute();
		}
		if (!res->result->success) {
			throw std::runtime_error(res->result->error);
		}
		return res->FetchDF();
	}

	py::object ToArrowTable() {
		auto res = make_unique<DuckDBPyResult>();
		{
			py::gil_scoped_release release;
			res->result = rel->Execute();
		}
		if (!res->result->success) {
			throw std::runtime_error(res->result->error);
		}
		return res->FetchArrowTable();
	}

	unique_ptr<DuckDBPyRelation> Union(DuckDBPyRelation *other) {
		return make_unique<DuckDBPyRelation>(rel->Union(other->rel));
	}

	unique_ptr<DuckDBPyRelation> Except(DuckDBPyRelation *other) {
		return make_unique<DuckDBPyRelation>(rel->Except(other->rel));
	}

	unique_ptr<DuckDBPyRelation> Intersect(DuckDBPyRelation *other) {
		return make_unique<DuckDBPyRelation>(rel->Intersect(other->rel));
	}

	unique_ptr<DuckDBPyRelation> Join(DuckDBPyRelation *other, const string &condition) {
		return make_unique<DuckDBPyRelation>(rel->Join(other->rel, condition));
	}

	void WriteCsv(const string &file) {
		rel->WriteCSV(file);
	}

	static void WriteCsvDF(py::object df, const string &file) {
		return DefaultConnection()->FromDF(std::move(df))->WriteCsv(file);
	}

	// should this return a rel with the new view?
	unique_ptr<DuckDBPyRelation> CreateView(const string &view_name, bool replace = true) {
		rel->CreateView(view_name, replace);
		return make_unique<DuckDBPyRelation>(rel);
	}

	static unique_ptr<DuckDBPyRelation> CreateViewDf(py::object df, const string &view_name, bool replace = true) {
		return DefaultConnection()->FromDF(std::move(df))->CreateView(view_name, replace);
	}

	unique_ptr<DuckDBPyResult> Query(const string &view_name, const string &sql_query) {
		auto res = make_unique<DuckDBPyResult>();
		res->result = rel->Query(view_name, sql_query);
		if (!res->result->success) {
			throw std::runtime_error(res->result->error);
		}
		return res;
	}

	unique_ptr<DuckDBPyResult> Execute() {
		auto res = make_unique<DuckDBPyResult>();
		{
			py::gil_scoped_release release;
			res->result = rel->Execute();
		}
		if (!res->result->success) {
			throw std::runtime_error(res->result->error);
		}
		return res;
	}

	static unique_ptr<DuckDBPyResult> QueryDF(py::object df, const string &view_name, const string &sql_query) {
		return DefaultConnection()->FromDF(std::move(df))->Query(view_name, sql_query);
	}

	void InsertInto(const string &table) {
		rel->Insert(table);
	}

	void Insert(py::object params = py::list()) {
		vector<vector<Value>> values {DuckDBPyConnection::TransformPythonParamList(std::move(params))};
		rel->Insert(values);
	}

	void Create(const string &table) {
		rel->Create(table);
	}

	string Print() {
		std::string rel_res_string;
		{
			py::gil_scoped_release release;
			rel_res_string = rel->Limit(10)->Execute()->ToString();
		}

		return rel->ToString() + "\n---------------------\n-- Result Preview  --\n---------------------\n" +
		       rel_res_string + "\n";
	}

	py::object Getattr(const py::str &key) {
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
	PY_SQL_TOKEN_IDENTIFIER = 0,
	PY_SQL_TOKEN_NUMERIC_CONSTANT,
	PY_SQL_TOKEN_STRING_CONSTANT,
	PY_SQL_TOKEN_OPERATOR,
	PY_SQL_TOKEN_KEYWORD,
	PY_SQL_TOKEN_COMMENT
};

static py::object PyTokenize(const string &query) {
	auto tokens = Parser::Tokenize(query);
	py::list result;
	for (auto &token : tokens) {
		auto tuple = py::tuple(2);
		tuple[0] = token.start;
		switch (token.type) {
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER:
			tuple[1] = PY_SQL_TOKEN_IDENTIFIER;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_NUMERIC_CONSTANT:
			tuple[1] = PY_SQL_TOKEN_NUMERIC_CONSTANT;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT:
			tuple[1] = PY_SQL_TOKEN_STRING_CONSTANT;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR:
			tuple[1] = PY_SQL_TOKEN_OPERATOR;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD:
			tuple[1] = PY_SQL_TOKEN_KEYWORD;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT:
			tuple[1] = PY_SQL_TOKEN_COMMENT;
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

	m.def("connect", &DuckDBPyConnection::Connect,
	      "Create a DuckDB database instance. Can take a database file name to read/write persistent data and a "
	      "read_only flag if no changes are desired",
	      py::arg("database") = ":memory:", py::arg("read_only") = false);
	m.def("tokenize", PyTokenize,
	      "Tokenizes a SQL string, returning a list of (position, type) tuples that can be "
	      "used for e.g. syntax highlighting",
	      py::arg("query"));
	py::enum_<PySQLTokenType>(m, "token_type")
	    .value("identifier", PySQLTokenType::PY_SQL_TOKEN_IDENTIFIER)
	    .value("numeric_const", PySQLTokenType::PY_SQL_TOKEN_NUMERIC_CONSTANT)
	    .value("string_const", PySQLTokenType::PY_SQL_TOKEN_STRING_CONSTANT)
	    .value("operator", PySQLTokenType::PY_SQL_TOKEN_OPERATOR)
	    .value("keyword", PySQLTokenType::PY_SQL_TOKEN_KEYWORD)
	    .value("comment", PySQLTokenType::PY_SQL_TOKEN_COMMENT)
	    .export_values();

	auto conn_class =
	    py::class_<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>>(m, "DuckDBPyConnection")
	        .def("cursor", &DuckDBPyConnection::Cursor, "Create a duplicate of the current connection")
	        .def("duplicate", &DuckDBPyConnection::Cursor, "Create a duplicate of the current connection")
	        .def("execute", &DuckDBPyConnection::Execute,
	             "Execute the given SQL query, optionally using prepared statements with parameters set",
	             py::arg("query"), py::arg("parameters") = py::list(), py::arg("multiple_parameter_sets") = false)
	        .def("executemany", &DuckDBPyConnection::ExecuteMany,
	             "Execute the given prepared statement multiple times using the list of parameter sets in parameters",
	             py::arg("query"), py::arg("parameters") = py::list())
	        .def("close", &DuckDBPyConnection::Close, "Close the connection")
	        .def("fetchone", &DuckDBPyConnection::FetchOne, "Fetch a single row from a result following execute")
	        .def("fetchall", &DuckDBPyConnection::FetchAll, "Fetch all rows from a result following execute")
	        .def("fetchnumpy", &DuckDBPyConnection::FetchNumpy,
	             "Fetch a result as list of NumPy arrays following execute")
	        .def("fetchdf", &DuckDBPyConnection::FetchDF, "Fetch a result as Data.Frame following execute()")
	        .def("fetch_df", &DuckDBPyConnection::FetchDF, "Fetch a result as Data.Frame following execute()")
	        .def("fetch_df_chunk", &DuckDBPyConnection::FetchDFChunk,
	             "Fetch a chunk of the result as Data.Frame following execute()")
	        .def("df", &DuckDBPyConnection::FetchDF, "Fetch a result as Data.Frame following execute()")
	        .def("fetch_arrow_table", &DuckDBPyConnection::FetchArrow,
	             "Fetch a result as Arrow table following execute()")
	        .def("arrow", &DuckDBPyConnection::FetchArrow, "Fetch a result as Arrow table following execute()")
	        .def("begin", &DuckDBPyConnection::Begin, "Start a new transaction")
	        .def("commit", &DuckDBPyConnection::Commit, "Commit changes performed within a transaction")
	        .def("rollback", &DuckDBPyConnection::Rollback, "Roll back changes performed within a transaction")
	        .def("append", &DuckDBPyConnection::Append, "Append the passed Data.Frame to the named table",
	             py::arg("table_name"), py::arg("df"))
	        .def("register", &DuckDBPyConnection::RegisterDF,
	             "Register the passed Data.Frame value for querying with a view", py::arg("view_name"), py::arg("df"))
	        .def("unregister", &DuckDBPyConnection::UnregisterDF, "Unregister the view name", py::arg("view_name"))
	        .def("table", &DuckDBPyConnection::Table, "Create a relation object for the name'd table",
	             py::arg("table_name"))
	        .def("view", &DuckDBPyConnection::View, "Create a relation object for the name'd view",
	             py::arg("view_name"))
	        .def("values", &DuckDBPyConnection::Values, "Create a relation object from the passed values",
	             py::arg("values"))
	        .def("table_function", &DuckDBPyConnection::TableFunction,
	             "Create a relation object from the name'd table function with given parameters", py::arg("name"),
	             py::arg("parameters") = py::list())
	        .def("from_df", &DuckDBPyConnection::FromDF, "Create a relation object from the Data.Frame in df",
	             py::arg("df"))
	        .def("from_arrow_table", &DuckDBPyConnection::FromArrowTable,
	             "Create a relation object from an Arrow table", py::arg("table"))
	        .def("df", &DuckDBPyConnection::FromDF,
	             "Create a relation object from the Data.Frame in df (alias of from_df)", py::arg("df"))
	        .def("from_csv_auto", &DuckDBPyConnection::FromCsvAuto,
	             "Create a relation object from the CSV file in file_name", py::arg("file_name"))
	        .def("from_parquet", &DuckDBPyConnection::FromParquet,
	             "Create a relation object from the Parquet file in file_name", py::arg("file_name"))
	        .def("__getattr__", &DuckDBPyConnection::GetAttr, "Get result set attributes, mainly column names");

	py::class_<DuckDBPyResult>(m, "DuckDBPyResult")
	    .def("close", &DuckDBPyResult::Close)
	    .def("fetchone", &DuckDBPyResult::Fetchone)
	    .def("fetchall", &DuckDBPyResult::Fetchall)
	    .def("fetchnumpy", &DuckDBPyResult::FetchNumpy)
	    .def("fetchdf", &DuckDBPyResult::FetchDF)
	    .def("fetch_df", &DuckDBPyResult::FetchDF)
	    .def("fetch_df_chunk", &DuckDBPyResult::FetchDFChunk)
	    .def("fetch_arrow_table", &DuckDBPyResult::FetchArrowTable)
	    .def("arrow", &DuckDBPyResult::FetchArrowTable)
	    .def("df", &DuckDBPyResult::FetchDF);

	py::class_<DuckDBPyRelation>(m, "DuckDBPyRelation")
	    .def("filter", &DuckDBPyRelation::Filter, "Filter the relation object by the filter in filter_expr",
	         py::arg("filter_expr"))
	    .def("project", &DuckDBPyRelation::Project, "Project the relation object by the projection in project_expr",
	         py::arg("project_expr"))
	    .def("set_alias", &DuckDBPyRelation::Alias, "Rename the relation object to new alias", py::arg("alias"))
	    .def("order", &DuckDBPyRelation::Order, "Reorder the relation object by order_expr", py::arg("order_expr"))
	    .def("aggregate", &DuckDBPyRelation::Aggregate,
	         "Compute the aggregate aggr_expr by the optional groups group_expr on the relation", py::arg("aggr_expr"),
	         py::arg("group_expr") = "")
	    .def("union", &DuckDBPyRelation::Union,
	         "Create the set union of this relation object with another relation object in other_rel")
	    .def("except_", &DuckDBPyRelation::Except,
	         "Create the set except of this relation object with another relation object in other_rel",
	         py::arg("other_rel"))
	    .def("intersect", &DuckDBPyRelation::Intersect,
	         "Create the set intersection of this relation object with another relation object in other_rel",
	         py::arg("other_rel"))
	    .def("join", &DuckDBPyRelation::Join,
	         "Join the relation object with another relation object in other_rel using the join condition expression "
	         "in join_condition",
	         py::arg("other_rel"), py::arg("join_condition"))
	    .def("distinct", &DuckDBPyRelation::Distinct, "Retrieve distinct rows from this relation object")
	    .def("limit", &DuckDBPyRelation::Limit, "Only retrieve the first n rows from this relation object",
	         py::arg("n"))
	    .def("query", &DuckDBPyRelation::Query,
	         "Run the given SQL query in sql_query on the view named virtual_table_name that refers to the relation "
	         "object",
	         py::arg("virtual_table_name"), py::arg("sql_query"))
	    .def("execute", &DuckDBPyRelation::Execute, "Transform the relation into a result set")
	    .def("write_csv", &DuckDBPyRelation::WriteCsv, "Write the relation object to a CSV file in file_name",
	         py::arg("file_name"))
	    .def("insert_into", &DuckDBPyRelation::InsertInto,
	         "Inserts the relation object into an existing table named table_name", py::arg("table_name"))
	    .def("insert", &DuckDBPyRelation::Insert, "Inserts the given values into the relation", py::arg("values"))
	    .def("create", &DuckDBPyRelation::Create,
	         "Creates a new table named table_name with the contents of the relation object", py::arg("table_name"))
	    .def("create_view", &DuckDBPyRelation::CreateView,
	         "Creates a view named view_name that refers to the relation object", py::arg("view_name"),
	         py::arg("replace") = true)
	    .def("to_arrow_table", &DuckDBPyRelation::ToArrowTable, "Transforms the relation object into a Arrow table")
	    .def("arrow", &DuckDBPyRelation::ToArrowTable, "Transforms the relation object into a Arrow table")
	    .def("to_df", &DuckDBPyRelation::ToDF, "Transforms the relation object into a Data.Frame")
	    .def("df", &DuckDBPyRelation::ToDF, "Transforms the relation object into a Data.Frame")
	    .def("__str__", &DuckDBPyRelation::Print)
	    .def("__repr__", &DuckDBPyRelation::Print)
	    .def("__getattr__", &DuckDBPyRelation::Getattr);

	m.def("values", &DuckDBPyRelation::Values, "Create a relation object from the passed values", py::arg("values"));
	m.def("from_csv_auto", &DuckDBPyRelation::FromCsvAuto, "Creates a relation object from the CSV file in file_name",
	      py::arg("file_name"));
	m.def("from_parquet", &DuckDBPyRelation::FromParquet,
	      "Creates a relation object from the Parquet file in file_name", py::arg("file_name"));
	m.def("df", &DuckDBPyRelation::FromDf, "Create a relation object from the Data.Frame df", py::arg("df"));
	m.def("from_df", &DuckDBPyRelation::FromDf, "Create a relation object from the Data.Frame df", py::arg("df"));
	m.def("from_arrow_table", &DuckDBPyRelation::FromArrowTable, "Create a relation object from an Arrow table",
	      py::arg("table"));
	m.def("arrow", &DuckDBPyRelation::FromArrowTable, "Create a relation object from an Arrow table", py::arg("table"));
	m.def("filter", &DuckDBPyRelation::FilterDf, "Filter the Data.Frame df by the filter in filter_expr", py::arg("df"),
	      py::arg("filter_expr"));
	m.def("project", &DuckDBPyRelation::ProjectDf, "Project the Data.Frame df by the projection in project_expr",
	      py::arg("df"), py::arg("project_expr"));
	m.def("alias", &DuckDBPyRelation::AliasDF, "Create a relation from Data.Frame df with the passed alias",
	      py::arg("df"), py::arg("alias"));
	m.def("order", &DuckDBPyRelation::OrderDf, "Reorder the Data.Frame df by order_expr", py::arg("df"),
	      py::arg("order_expr"));
	m.def("aggregate", &DuckDBPyRelation::AggregateDF,
	      "Compute the aggregate aggr_expr by the optional groups group_expr on Data.frame df", py::arg("df"),
	      py::arg("aggr_expr"), py::arg("group_expr") = "");
	m.def("distinct", &DuckDBPyRelation::DistinctDF, "Compute the distinct rows from Data.Frame df ", py::arg("df"));
	m.def("limit", &DuckDBPyRelation::LimitDF, "Retrieve the first n rows from the Data.Frame df", py::arg("df"),
	      py::arg("n"));
	m.def("query", &DuckDBPyRelation::QueryDF,
	      "Run the given SQL query in sql_query on the view named virtual_table_name that contains the content of "
	      "Data.Frame df",
	      py::arg("df"), py::arg("virtual_table_name"), py::arg("sql_query"));
	m.def("write_csv", &DuckDBPyRelation::WriteCsvDF, "Write the Data.Frame df to a CSV file in file_name",
	      py::arg("df"), py::arg("file_name"));

	// we need this because otherwise we try to remove registered_dfs on shutdown when python is already dead
	auto clean_default_connection = []() {
		default_connection.reset();
	};
	m.add_object("_clean_default_connection", py::capsule(clean_default_connection));
	PyDateTime_IMPORT;
}

} // namespace duckdb
