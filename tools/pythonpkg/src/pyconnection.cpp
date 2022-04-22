#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pandas_scan.hpp"
#include "duckdb_python/map.hpp"

#include "duckdb/common/arrow.hpp"
#include "duckdb_python/arrow_array_stream.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/parser.hpp"

#include "datetime.h" // from Python

#include <random>

namespace duckdb {

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::default_connection = nullptr;

void DuckDBPyConnection::Initialize(py::handle &m) {
	py::class_<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>>(m, "DuckDBPyConnection", py::module_local())
	    .def("cursor", &DuckDBPyConnection::Cursor, "Create a duplicate of the current connection")
	    .def("duplicate", &DuckDBPyConnection::Cursor, "Create a duplicate of the current connection")
	    .def("execute", &DuckDBPyConnection::Execute,
	         "Execute the given SQL query, optionally using prepared statements with parameters set", py::arg("query"),
	         py::arg("parameters") = py::list(), py::arg("multiple_parameter_sets") = false)
	    .def("executemany", &DuckDBPyConnection::ExecuteMany,
	         "Execute the given prepared statement multiple times using the list of parameter sets in parameters",
	         py::arg("query"), py::arg("parameters") = py::list())
	    .def("close", &DuckDBPyConnection::Close, "Close the connection")
	    .def("fetchone", &DuckDBPyConnection::FetchOne, "Fetch a single row from a result following execute")
	    .def("fetchall", &DuckDBPyConnection::FetchAll, "Fetch all rows from a result following execute")
	    .def("fetchnumpy", &DuckDBPyConnection::FetchNumpy, "Fetch a result as list of NumPy arrays following execute")
	    .def("fetchdf", &DuckDBPyConnection::FetchDF, "Fetch a result as Data.Frame following execute()")
	    .def("fetch_df", &DuckDBPyConnection::FetchDF, "Fetch a result as Data.Frame following execute()")
	    .def("fetch_df_chunk", &DuckDBPyConnection::FetchDFChunk,
	         "Fetch a chunk of the result as Data.Frame following execute()", py::arg("vectors_per_chunk") = 1)
	    .def("df", &DuckDBPyConnection::FetchDF, "Fetch a result as Data.Frame following execute()")
	    .def("fetch_arrow_table", &DuckDBPyConnection::FetchArrow, "Fetch a result as Arrow table following execute()",
	         py::arg("chunk_size") = 1000000)
	    .def("fetch_record_batch", &DuckDBPyConnection::FetchRecordBatchReader,
	         "Fetch an Arrow RecordBatchReader following execute()", py::arg("chunk_size") = 1000000)
	    .def("arrow", &DuckDBPyConnection::FetchArrow, "Fetch a result as Arrow table following execute()",
	         py::arg("chunk_size") = 1000000)
	    .def("begin", &DuckDBPyConnection::Begin, "Start a new transaction")
	    .def("commit", &DuckDBPyConnection::Commit, "Commit changes performed within a transaction")
	    .def("rollback", &DuckDBPyConnection::Rollback, "Roll back changes performed within a transaction")
	    .def("append", &DuckDBPyConnection::Append, "Append the passed Data.Frame to the named table",
	         py::arg("table_name"), py::arg("df"))
	    .def("register", &DuckDBPyConnection::RegisterPythonObject,
	         "Register the passed Python Object value for querying with a view", py::arg("view_name"),
	         py::arg("python_object"), py::arg("rows_per_thread") = 1000000)
	    .def("unregister", &DuckDBPyConnection::UnregisterPythonObject, "Unregister the view name",
	         py::arg("view_name"))
	    .def("table", &DuckDBPyConnection::Table, "Create a relation object for the name'd table",
	         py::arg("table_name"))
	    .def("view", &DuckDBPyConnection::View, "Create a relation object for the name'd view", py::arg("view_name"))
	    .def("values", &DuckDBPyConnection::Values, "Create a relation object from the passed values",
	         py::arg("values"))
	    .def("table_function", &DuckDBPyConnection::TableFunction,
	         "Create a relation object from the name'd table function with given parameters", py::arg("name"),
	         py::arg("parameters") = py::list())
	    .def("from_query", &DuckDBPyConnection::FromQuery, "Create a relation object from the given SQL query",
	         py::arg("query"), py::arg("alias") = "query_relation")
	    .def("query", &DuckDBPyConnection::RunQuery,
	         "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, "
	         "otherwise run the query as-is.",
	         py::arg("query"), py::arg("alias") = "query_relation")
	    .def("from_df", &DuckDBPyConnection::FromDF, "Create a relation object from the Data.Frame in df",
	         py::arg("df") = py::none())
	    .def("from_arrow", &DuckDBPyConnection::FromArrow, "Create a relation object from an Arrow object",
	         py::arg("arrow_object"), py::arg("rows_per_thread") = 1000000)
	    .def("df", &DuckDBPyConnection::FromDF, "Create a relation object from the Data.Frame in df (alias of from_df)",
	         py::arg("df"))
	    .def("from_csv_auto", &DuckDBPyConnection::FromCsvAuto,
	         "Create a relation object from the CSV file in file_name", py::arg("file_name"))
	    .def("from_parquet", &DuckDBPyConnection::FromParquet,
	         "Create a relation object from the Parquet file in file_name", py::arg("file_name"),
	         py::arg("binary_as_string") = false)
	    .def("from_substrait", &DuckDBPyConnection::FromSubstrait, "Create a query object from protobuf plan",
	         py::arg("proto"))
	    .def("get_substrait", &DuckDBPyConnection::GetSubstrait, "Serialize a query to protobuf", py::arg("query"))
	    .def_property_readonly("description", &DuckDBPyConnection::GetDescription,
	                           "Get result set attributes, mainly column names");

	PyDateTime_IMPORT;
}

DuckDBPyConnection *DuckDBPyConnection::ExecuteMany(const string &query, py::object params) {
	Execute(query, std::move(params), true);
	return this;
}

DuckDBPyConnection *DuckDBPyConnection::Execute(const string &query, py::object params, bool many) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	if (std::this_thread::get_id() != thread_id && check_same_thread) {
		throw std::runtime_error("DuckDB objects created in a thread can only be used in that same thread. The object "
		                         "was created in thread id " +
		                         to_string(std::hash<std::thread::id> {}(thread_id)) + " and this is thread id " +
		                         to_string(std::hash<std::thread::id> {}(std::this_thread::get_id())));
	}
	result = nullptr;
	unique_ptr<PreparedStatement> prep;
	{
		py::gil_scoped_release release;
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

		prep = connection->Prepare(move(statements.back()));
		if (!prep->success) {
			throw std::runtime_error(prep->error);
		}
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
			if (!res->result->success) {
				throw std::runtime_error(res->result->error);
			}
		}

		if (!many) {
			result = move(res);
		}
	}
	return this;
}

DuckDBPyConnection *DuckDBPyConnection::Append(const string &name, py::object value) {
	RegisterPythonObject("__append_df", std::move(value));
	return Execute("INSERT INTO \"" + name + "\" SELECT * FROM __append_df");
}

DuckDBPyConnection *DuckDBPyConnection::RegisterPythonObject(const string &name, py::object python_object,
                                                             const idx_t rows_per_tuple) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	auto py_object_type = string(py::str(python_object.get_type().attr("__name__")));

	if (py_object_type == "DataFrame") {
		{
			py::gil_scoped_release release;
			connection->TableFunction("pandas_scan", {Value::POINTER((uintptr_t)python_object.ptr())})
			    ->CreateView(name, true, true);
		}

		// keep a reference
		auto object = make_unique<RegisteredObject>(python_object);
		registered_objects[name] = move(object);
	} else if (IsAcceptedArrowObject(py_object_type)) {
		auto stream_factory = make_unique<PythonTableArrowArrayStreamFactory>(python_object.ptr());

		auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
		auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;
		{
			py::gil_scoped_release release;
			connection
			    ->TableFunction("arrow_scan",
			                    {Value::POINTER((uintptr_t)stream_factory.get()),
			                     Value::POINTER((uintptr_t)stream_factory_produce),
			                     Value::POINTER((uintptr_t)stream_factory_get_schema), Value::UBIGINT(rows_per_tuple)})
			    ->CreateView(name, true, true);
		}
		auto object = make_unique<RegisteredArrow>(move(stream_factory), move(python_object));
		registered_objects[name] = move(object);
	} else {
		throw std::runtime_error("Python Object " + py_object_type + " not suitable to be registered as a view");
	}
	return this;
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromQuery(const string &query, const string &alias) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	const char *duckdb_query_error = R"(duckdb.from_query cannot be used to run arbitrary SQL queries.
It can only be used to run individual SELECT statements, and converts the result of that SELECT
statement into a Relation object.
Use duckdb.query to run arbitrary SQL queries.)";
	return make_unique<DuckDBPyRelation>(connection->RelationFromQuery(query, alias, duckdb_query_error));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::RunQuery(const string &query, const string &alias) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	Parser parser(connection->context->GetParserOptions());
	parser.ParseQuery(query);
	if (parser.statements.size() == 1 && parser.statements[0]->type == StatementType::SELECT_STATEMENT) {
		return make_unique<DuckDBPyRelation>(connection->RelationFromQuery(
		    unique_ptr_cast<SQLStatement, SelectStatement>(move(parser.statements[0])), alias));
	}
	Execute(query);
	if (result) {
		FetchAll();
	}
	return nullptr;
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::Table(const string &tname) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	return make_unique<DuckDBPyRelation>(connection->Table(tname));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::Values(py::object params) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	vector<vector<Value>> values {DuckDBPyConnection::TransformPythonParamList(std::move(params))};
	return make_unique<DuckDBPyRelation>(connection->Values(values));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::View(const string &vname) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	return make_unique<DuckDBPyRelation>(connection->View(vname));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::TableFunction(const string &fname, py::object params) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}

	return make_unique<DuckDBPyRelation>(
	    connection->TableFunction(fname, DuckDBPyConnection::TransformPythonParamList(std::move(params))));
}

static std::string GenerateRandomName() {
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<> dis(0, 15);

	std::stringstream ss;
	int i;
	ss << std::hex;
	for (i = 0; i < 16; i++) {
		ss << dis(gen);
	}
	return ss.str();
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromDF(py::object value) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	string name = "df_" + GenerateRandomName();
	registered_objects[name] = make_unique<RegisteredObject>(value);
	vector<Value> params;
	params.emplace_back(Value::POINTER((uintptr_t)value.ptr()));
	return make_unique<DuckDBPyRelation>(connection->TableFunction("pandas_scan", params)->Alias(name));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromCsvAuto(const string &filename) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	vector<Value> params;
	params.emplace_back(filename);
	return make_unique<DuckDBPyRelation>(connection->TableFunction("read_csv_auto", params)->Alias(filename));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromParquet(const string &filename, bool binary_as_string) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	vector<Value> params;
	params.emplace_back(filename);
	named_parameter_map_t named_parameters({{"binary_as_string", Value::BOOLEAN(binary_as_string)}});
	return make_unique<DuckDBPyRelation>(
	    connection->TableFunction("parquet_scan", params, named_parameters)->Alias(filename));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromArrow(py::object &arrow_object, const idx_t rows_per_tuple) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	py::gil_scoped_acquire acquire;
	string name = "arrow_table_" + GenerateRandomName();
	auto py_object_type = string(py::str(arrow_object.get_type().attr("__name__")));
	if (!IsAcceptedArrowObject(py_object_type)) {
		throw std::runtime_error("Python Object Type " + py_object_type + " is not an accepted Arrow Object.");
	}
	auto stream_factory = make_unique<PythonTableArrowArrayStreamFactory>(arrow_object.ptr());

	auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
	auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;

	auto rel = make_unique<DuckDBPyRelation>(
	    connection
	        ->TableFunction("arrow_scan",
	                        {Value::POINTER((uintptr_t)stream_factory.get()),
	                         Value::POINTER((uintptr_t)stream_factory_produce),
	                         Value::POINTER((uintptr_t)stream_factory_get_schema), Value::UBIGINT(rows_per_tuple)})
	        ->Alias(name));
	registered_objects[name] = make_unique<RegisteredArrow>(move(stream_factory), arrow_object);
	return rel;
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromSubstrait(py::bytes &proto) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	string name = "substrait_" + GenerateRandomName();
	vector<Value> params;
	params.emplace_back(Value::BLOB_RAW(proto));
	return make_unique<DuckDBPyRelation>(connection->TableFunction("from_substrait", params)->Alias(name));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::GetSubstrait(const string &query) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	vector<Value> params;
	params.emplace_back(query);
	return make_unique<DuckDBPyRelation>(connection->TableFunction("get_substrait", params)->Alias(query));
}

DuckDBPyConnection *DuckDBPyConnection::UnregisterPythonObject(const string &name) {
	registered_objects.erase(name);
	py::gil_scoped_release release;
	if (connection) {
		connection->Query("DROP VIEW \"" + name + "\"");
	}
	return this;
}

DuckDBPyConnection *DuckDBPyConnection::Begin() {
	Execute("BEGIN TRANSACTION");
	return this;
}

DuckDBPyConnection *DuckDBPyConnection::Commit() {
	if (connection->context->transaction.IsAutoCommit()) {
		return this;
	}
	Execute("COMMIT");
	return this;
}

DuckDBPyConnection *DuckDBPyConnection::Rollback() {
	Execute("ROLLBACK");
	return this;
}

py::object DuckDBPyConnection::GetDescription() {
	if (!result) {
		return py::none();
	}
	return result->Description();
}

void DuckDBPyConnection::Close() {
	result = nullptr;
	connection = nullptr;
	database = nullptr;
	for (auto &cur : cursors) {
		cur->Close();
	}
	cursors.clear();
}

// cursor() is stupid
shared_ptr<DuckDBPyConnection> DuckDBPyConnection::Cursor() {
	auto res = make_shared<DuckDBPyConnection>(thread_id);
	res->database = database;
	res->connection = connection;
	cursors.push_back(res);
	return res;
}

// these should be functions on the result but well
py::object DuckDBPyConnection::FetchOne() {
	if (!result) {
		throw std::runtime_error("no open result set");
	}
	return result->Fetchone();
}

py::list DuckDBPyConnection::FetchAll() {
	if (!result) {
		throw std::runtime_error("no open result set");
	}
	return result->Fetchall();
}

py::dict DuckDBPyConnection::FetchNumpy() {
	if (!result) {
		throw std::runtime_error("no open result set");
	}
	return result->FetchNumpyInternal();
}
py::object DuckDBPyConnection::FetchDF() {
	if (!result) {
		throw std::runtime_error("no open result set");
	}
	return result->FetchDF();
}

py::object DuckDBPyConnection::FetchDFChunk(const idx_t vectors_per_chunk) const {
	if (!result) {
		throw std::runtime_error("no open result set");
	}
	return result->FetchDFChunk(vectors_per_chunk);
}

py::object DuckDBPyConnection::FetchArrow(idx_t chunk_size) {
	if (!result) {
		throw std::runtime_error("no open result set");
	}
	return result->FetchArrowTable(chunk_size);
}

py::object DuckDBPyConnection::FetchRecordBatchReader(const idx_t chunk_size) const {
	if (!result) {
		throw std::runtime_error("no open result set");
	}
	return result->FetchRecordBatchReader(chunk_size);
}
static unique_ptr<TableFunctionRef>
TryReplacement(py::dict &dict, py::str &table_name,
               unordered_map<string, unique_ptr<RegisteredObject>> &registered_objects) {
	if (!dict.contains(table_name)) {
		// not present in the globals
		return nullptr;
	}
	auto entry = dict[table_name];
	auto py_object_type = string(py::str(entry.get_type().attr("__name__")));
	auto table_function = make_unique<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	if (py_object_type == "DataFrame") {
		string name = "df_" + GenerateRandomName();
		children.push_back(make_unique<ConstantExpression>(Value::POINTER((uintptr_t)entry.ptr())));
		table_function->function = make_unique<FunctionExpression>("pandas_scan", move(children));
		// keep a reference
		auto object = make_unique<RegisteredObject>(entry);
		registered_objects[name] = move(object);
	} else if (DuckDBPyConnection::IsAcceptedArrowObject(py_object_type)) {
		string name = "arrow_" + GenerateRandomName();
		auto stream_factory = make_unique<PythonTableArrowArrayStreamFactory>(entry.ptr());
		auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
		auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;

		children.push_back(make_unique<ConstantExpression>(Value::POINTER((uintptr_t)stream_factory.get())));
		children.push_back(make_unique<ConstantExpression>(Value::POINTER((uintptr_t)stream_factory_produce)));
		children.push_back(make_unique<ConstantExpression>(Value::POINTER((uintptr_t)stream_factory_get_schema)));
		children.push_back(make_unique<ConstantExpression>(Value::UBIGINT(1000000)));
		table_function->function = make_unique<FunctionExpression>("arrow_scan", move(children));
		registered_objects[name] = make_unique<RegisteredArrow>(move(stream_factory), entry);
	} else {
		throw std::runtime_error("Python Object " + py_object_type + " not suitable for replacement scans");
	}
	return table_function;
}

struct ReplacementRegisteredObjects : public ReplacementScanData {
	unordered_map<string, unique_ptr<RegisteredObject>> *registered_objects;
};

static unique_ptr<TableFunctionRef> ScanReplacement(ClientContext &context, const string &table_name,
                                                    ReplacementScanData *data) {
	py::gil_scoped_acquire acquire;
	auto &registered_data = (ReplacementRegisteredObjects &)*data;
	auto registered_objects = registered_data.registered_objects;
	auto py_table_name = py::str(table_name);
	// Here we do an exhaustive search on the frame lineage
	auto current_frame = py::module::import("inspect").attr("currentframe")();
	while (hasattr(current_frame, "f_locals")) {
		auto local_dict = py::reinterpret_borrow<py::dict>(current_frame.attr("f_locals"));
		// search local dictionary
		if (local_dict) {
			auto result = TryReplacement(local_dict, py_table_name, *registered_objects);
			if (result) {
				return result;
			}
		}
		// search global dictionary
		auto global_dict = py::reinterpret_borrow<py::dict>(current_frame.attr("f_globals"));
		if (global_dict) {
			auto result = TryReplacement(global_dict, py_table_name, *registered_objects);
			if (result) {
				return result;
			}
		}
		current_frame = current_frame.attr("f_back");
	}
	// Not found :(
	return nullptr;
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::Connect(const string &database, bool read_only,
                                                           const py::dict &config_dict, bool check_same_thread) {
	auto res = make_shared<DuckDBPyConnection>();
	DBConfig config;
	if (read_only) {
		config.access_mode = AccessMode::READ_ONLY;
	}
	for (auto &kv : config_dict) {
		string key = py::str(kv.first);
		string val = py::str(kv.second);
		auto config_property = DBConfig::GetOptionByName(key);
		if (!config_property) {
			throw InvalidInputException("Unrecognized configuration property \"%s\"", key);
		}
		config.SetOption(*config_property, Value(val));
	}
	if (config.enable_external_access) {
		auto extra_data = make_unique<ReplacementRegisteredObjects>();
		extra_data->registered_objects = &res->registered_objects;
		config.replacement_scans.emplace_back(ScanReplacement, move(extra_data));
	}

	res->database = make_unique<DuckDB>(database, &config);
	res->connection = make_unique<Connection>(*res->database);
	res->check_same_thread = check_same_thread;
	PandasScanFunction scan_fun;
	CreateTableFunctionInfo scan_info(scan_fun);

	MapFunction map_fun;
	CreateTableFunctionInfo map_info(map_fun);

	auto &context = *res->connection->context;
	auto &catalog = Catalog::GetCatalog(context);
	context.transaction.BeginTransaction();
	catalog.CreateTableFunction(context, &scan_info);
	catalog.CreateTableFunction(context, &map_info);

	context.transaction.Commit();

	return res;
}

Value TransformPythonValue(py::handle ele) {
	auto datetime_mod = py::module::import("datetime");
	auto datetime_date = datetime_mod.attr("date");
	auto datetime_datetime = datetime_mod.attr("datetime");
	auto datetime_time = datetime_mod.attr("time");
	auto decimal_mod = py::module::import("decimal");
	auto decimal_decimal = decimal_mod.attr("Decimal");

	if (ele.is_none()) {
		return Value();
	} else if (py::isinstance<py::bool_>(ele)) {
		return Value::BOOLEAN(ele.cast<bool>());
	} else if (py::isinstance<py::int_>(ele)) {
		return Value::BIGINT(ele.cast<int64_t>());
	} else if (py::isinstance<py::float_>(ele)) {
		return Value::DOUBLE(ele.cast<double>());
	} else if (py::isinstance(ele, decimal_decimal)) {
		return py::str(ele).cast<string>();
	} else if (py::isinstance(ele, datetime_datetime)) {
		auto ptr = ele.ptr();
		auto year = PyDateTime_GET_YEAR(ptr);
		auto month = PyDateTime_GET_MONTH(ptr);
		auto day = PyDateTime_GET_DAY(ptr);
		auto hour = PyDateTime_DATE_GET_HOUR(ptr);
		auto minute = PyDateTime_DATE_GET_MINUTE(ptr);
		auto second = PyDateTime_DATE_GET_SECOND(ptr);
		auto micros = PyDateTime_DATE_GET_MICROSECOND(ptr);
		return Value::TIMESTAMP(year, month, day, hour, minute, second, micros);
	} else if (py::isinstance(ele, datetime_time)) {
		auto ptr = ele.ptr();
		auto hour = PyDateTime_TIME_GET_HOUR(ptr);
		auto minute = PyDateTime_TIME_GET_MINUTE(ptr);
		auto second = PyDateTime_TIME_GET_SECOND(ptr);
		auto micros = PyDateTime_TIME_GET_MICROSECOND(ptr);
		return Value::TIME(hour, minute, second, micros);
	} else if (py::isinstance(ele, datetime_date)) {
		auto ptr = ele.ptr();
		auto year = PyDateTime_GET_YEAR(ptr);
		auto month = PyDateTime_GET_MONTH(ptr);
		auto day = PyDateTime_GET_DAY(ptr);
		return Value::DATE(year, month, day);
	} else if (py::isinstance<py::str>(ele)) {
		return ele.cast<string>();
	} else if (py::isinstance<py::memoryview>(ele)) {
		py::memoryview py_view = ele.cast<py::memoryview>();
		PyObject *py_view_ptr = py_view.ptr();
		Py_buffer *py_buf = PyMemoryView_GET_BUFFER(py_view_ptr);
		return Value::BLOB(const_data_ptr_t(py_buf->buf), idx_t(py_buf->len));
	} else if (py::isinstance<py::bytes>(ele)) {
		const string &ele_string = ele.cast<string>();
		return Value::BLOB(const_data_ptr_t(ele_string.data()), ele_string.size());
	} else if (py::isinstance<py::list>(ele)) {
		auto size = py::len(ele);

		if (size == 0) {
			return Value::EMPTYLIST(LogicalType::SQLNULL);
		}

		vector<Value> values;
		values.reserve(size);

		for (auto py_val : ele) {
			values.emplace_back(TransformPythonValue(py_val));
		}

		return Value::LIST(values);
	} else {
		throw std::runtime_error("unknown param type " + py::str(ele.get_type()).cast<string>());
	}
}

vector<Value> DuckDBPyConnection::TransformPythonParamList(py::handle params) {
	vector<Value> args;
	args.reserve(py::len(params));

	for (auto param : params) {
		args.emplace_back(TransformPythonValue(param));
	}
	return args;
}

DuckDBPyConnection *DuckDBPyConnection::DefaultConnection() {
	if (!default_connection) {
		py::dict config_dict;
		default_connection = DuckDBPyConnection::Connect(":memory:", false, config_dict, true);
	}
	return default_connection.get();
}

void DuckDBPyConnection::Cleanup() {
	default_connection.reset();
}

bool DuckDBPyConnection::IsAcceptedArrowObject(string &py_object_type) {
	if (py_object_type == "Table" || py_object_type == "FileSystemDataset" || py_object_type == "InMemoryDataset" ||
	    py_object_type == "RecordBatchReader" || py_object_type == "Scanner") {
		return true;
	}
	return false;
}

} // namespace duckdb
