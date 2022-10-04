#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/python_conversion.hpp"
#include "duckdb_python/pandas_scan.hpp"
#include "duckdb_python/map.hpp"

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb_python/arrow_array_stream.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb_python/python_conversion.hpp"

#include "datetime.h" // from Python

#include <random>

namespace duckdb {

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::default_connection = nullptr;
shared_ptr<PythonImportCache> DuckDBPyConnection::import_cache = nullptr;

void DuckDBPyConnection::Initialize(py::handle &m) {
	py::class_<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>>(m, "DuckDBPyConnection", py::module_local())
	    .def("cursor", &DuckDBPyConnection::Cursor, "Create a duplicate of the current connection")
	    .def("duplicate", &DuckDBPyConnection::Cursor, "Create a duplicate of the current connection")
	    .def("execute", &DuckDBPyConnection::Execute,
	         "Execute the given SQL query, optionally using prepared statements with parameters set", py::arg("query"),
	         py::arg("parameters") = py::none(), py::arg("multiple_parameter_sets") = false)
	    .def("executemany", &DuckDBPyConnection::ExecuteMany,
	         "Execute the given prepared statement multiple times using the list of parameter sets in parameters",
	         py::arg("query"), py::arg("parameters") = py::none())
	    .def("close", &DuckDBPyConnection::Close, "Close the connection")
	    .def("fetchone", &DuckDBPyConnection::FetchOne, "Fetch a single row from a result following execute")
	    .def("fetchmany", &DuckDBPyConnection::FetchMany, "Fetch the next set of rows from a result following execute",
	         py::arg("size") = 1)
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
	         py::arg("python_object"))
	    .def("unregister", &DuckDBPyConnection::UnregisterPythonObject, "Unregister the view name",
	         py::arg("view_name"))
	    .def("table", &DuckDBPyConnection::Table, "Create a relation object for the name'd table",
	         py::arg("table_name"))
	    .def("view", &DuckDBPyConnection::View, "Create a relation object for the name'd view", py::arg("view_name"))
	    .def("values", &DuckDBPyConnection::Values, "Create a relation object from the passed values",
	         py::arg("values"))
	    .def("table_function", &DuckDBPyConnection::TableFunction,
	         "Create a relation object from the name'd table function with given parameters", py::arg("name"),
	         py::arg("parameters") = py::none())
	    .def("from_query", &DuckDBPyConnection::FromQuery, "Create a relation object from the given SQL query",
	         py::arg("query"), py::arg("alias") = "query_relation")
	    .def("query", &DuckDBPyConnection::RunQuery,
	         "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, "
	         "otherwise run the query as-is.",
	         py::arg("query"), py::arg("alias") = "query_relation")
	    .def("from_df", &DuckDBPyConnection::FromDF, "Create a relation object from the Data.Frame in df",
	         py::arg("df") = py::none())
	    .def("from_arrow", &DuckDBPyConnection::FromArrow, "Create a relation object from an Arrow object",
	         py::arg("arrow_object"))
	    .def("df", &DuckDBPyConnection::FromDF,
	         "Create a relation object from the Data.Frame in df. This is an alias of from_df", py::arg("df"))
	    .def("from_csv_auto", &DuckDBPyConnection::FromCsvAuto,
	         "Create a relation object from the CSV file in file_name", py::arg("file_name"))
	    .def("from_parquet", &DuckDBPyConnection::FromParquet,
	         "Create a relation object from the Parquet file in file_name", py::arg("file_name"),
	         py::arg("binary_as_string") = false)
	    .def("from_substrait", &DuckDBPyConnection::FromSubstrait, "Create a query object from protobuf plan",
	         py::arg("proto"))
	    .def("get_substrait", &DuckDBPyConnection::GetSubstrait, "Serialize a query to protobuf", py::arg("query"))
	    .def("get_substrait_json", &DuckDBPyConnection::GetSubstraitJSON,
	         "Serialize a query to protobuf on the JSON format", py::arg("query"))
	    .def("get_table_names", &DuckDBPyConnection::GetTableNames, "Extract the required table names from a query",
	         py::arg("query"))
	    .def("__enter__", &DuckDBPyConnection::Enter)
	    .def("__exit__", &DuckDBPyConnection::Exit, py::arg("exc_type"), py::arg("exc"), py::arg("traceback"))
	    .def_property_readonly("description", &DuckDBPyConnection::GetDescription,
	                           "Get result set attributes, mainly column names")
	    .def("install_extension", &DuckDBPyConnection::InstallExtension, "Install an extension by name",
	         py::arg("extension"), py::kw_only(), py::arg("force_install") = false)
	    .def("load_extension", &DuckDBPyConnection::LoadExtension, "Load an installed extension", py::arg("extension"));

	PyDateTime_IMPORT;
	DuckDBPyConnection::ImportCache();
}

DuckDBPyConnection *DuckDBPyConnection::ExecuteMany(const string &query, py::object params) {
	if (params.is_none()) {
		params = py::list();
	}
	Execute(query, std::move(params), true);
	return this;
}

static unique_ptr<QueryResult> CompletePendingQuery(PendingQueryResult &pending_query) {
	PendingExecutionResult execution_result;
	do {
		execution_result = pending_query.ExecuteTask();
	} while (execution_result == PendingExecutionResult::RESULT_NOT_READY);
	if (execution_result == PendingExecutionResult::EXECUTION_ERROR) {
		pending_query.ThrowError();
	}
	return pending_query.Execute();
}

DuckDBPyConnection *DuckDBPyConnection::Execute(const string &query, py::object params, bool many) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	if (params.is_none()) {
		params = py::list();
	}
	result = nullptr;
	unique_ptr<PreparedStatement> prep;
	{
		py::gil_scoped_release release;
		unique_lock<std::mutex> lock(py_connection_lock);

		auto statements = connection->ExtractStatements(query);
		if (statements.empty()) {
			// no statements to execute
			return this;
		}
		// if there are multiple statements, we directly execute the statements besides the last one
		// we only return the result of the last statement to the user, unless one of the previous statements fails
		for (idx_t i = 0; i + 1 < statements.size(); i++) {
			auto pending_query = connection->PendingQuery(move(statements[i]));
			auto res = CompletePendingQuery(*pending_query);

			if (res->HasError()) {
				res->ThrowError();
			}
		}

		prep = connection->Prepare(move(statements.back()));
		if (prep->HasError()) {
			prep->error.Throw();
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
			throw InvalidInputException("Prepared statement needs %d parameters, %d given", prep->n_param,
			                            py::len(single_query_params));
		}
		auto args = DuckDBPyConnection::TransformPythonParamList(single_query_params);
		auto res = make_unique<DuckDBPyResult>();
		{
			py::gil_scoped_release release;
			unique_lock<std::mutex> lock(py_connection_lock);
			auto pending_query = prep->PendingQuery(args);
			res->result = CompletePendingQuery(*pending_query);

			if (res->result->HasError()) {
				res->result->ThrowError();
			}
		}

		if (!many) {
			result = move(res);
		}
	}
	return this;
}

DuckDBPyConnection *DuckDBPyConnection::Append(const string &name, DataFrame value) {
	RegisterPythonObject("__append_df", std::move(value));
	return Execute("INSERT INTO \"" + name + "\" SELECT * FROM __append_df");
}

DuckDBPyConnection *DuckDBPyConnection::RegisterPythonObject(const string &name, py::object python_object) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	auto py_object_type = string(py::str(python_object.get_type().attr("__name__")));

	if (py_object_type == "DataFrame") {
		auto new_df = PandasScanFunction::PandasReplaceCopiedNames(python_object);
		{
			py::gil_scoped_release release;
			temporary_views[name] = connection->TableFunction("pandas_scan", {Value::POINTER((uintptr_t)new_df.ptr())})
			                            ->CreateView(name, true, true);
		}

		// keep a reference
		vector<shared_ptr<ExternalDependency>> dependencies;
		dependencies.push_back(make_shared<PythonDependencies>(make_unique<RegisteredObject>(python_object),
		                                                       make_unique<RegisteredObject>(new_df)));
		connection->context->external_dependencies[name] = move(dependencies);
	} else if (IsAcceptedArrowObject(py_object_type)) {
		auto stream_factory =
		    make_unique<PythonTableArrowArrayStreamFactory>(python_object.ptr(), connection->context->config);
		auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
		auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;
		{
			py::gil_scoped_release release;
			temporary_views[name] =
			    connection
			        ->TableFunction("arrow_scan", {Value::POINTER((uintptr_t)stream_factory.get()),
			                                       Value::POINTER((uintptr_t)stream_factory_produce),
			                                       Value::POINTER((uintptr_t)stream_factory_get_schema)})
			        ->CreateView(name, true, true);
		}
		vector<shared_ptr<ExternalDependency>> dependencies;
		dependencies.push_back(
		    make_shared<PythonDependencies>(make_unique<RegisteredArrow>(move(stream_factory), python_object)));
		connection->context->external_dependencies[name] = move(dependencies);
	} else {
		throw InvalidInputException("Python Object %s not suitable to be registered as a view", py_object_type);
	}
	return this;
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromQuery(const string &query, const string &alias) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	const char *duckdb_query_error = R"(duckdb.from_query cannot be used to run arbitrary SQL queries.
It can only be used to run individual SELECT statements, and converts the result of that SELECT
statement into a Relation object.
Use duckdb.query to run arbitrary SQL queries.)";
	return make_unique<DuckDBPyRelation>(connection->RelationFromQuery(query, alias, duckdb_query_error));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::RunQuery(const string &query, const string &alias) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
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
		throw ConnectionException("Connection has already been closed");
	}
	return make_unique<DuckDBPyRelation>(connection->Table(tname));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::Values(py::object params) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	if (params.is_none()) {
		params = py::list();
	}
	if (!py::hasattr(params, "__len__")) {
		throw InvalidInputException("Type of object passed to parameter 'values' must be iterable");
	}
	vector<vector<Value>> values {DuckDBPyConnection::TransformPythonParamList(std::move(params))};
	return make_unique<DuckDBPyRelation>(connection->Values(values));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::View(const string &vname) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	// First check our temporary view
	if (temporary_views.find(vname) != temporary_views.end()) {
		return make_unique<DuckDBPyRelation>(temporary_views[vname]);
	}
	return make_unique<DuckDBPyRelation>(connection->View(vname));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::TableFunction(const string &fname, py::object params) {
	if (params.is_none()) {
		params = py::list();
	}
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
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

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromDF(const DataFrame &value) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	string name = "df_" + GenerateRandomName();
	auto new_df = PandasScanFunction::PandasReplaceCopiedNames(value);
	vector<Value> params;
	params.emplace_back(Value::POINTER((uintptr_t)new_df.ptr()));
	auto rel = make_unique<DuckDBPyRelation>(connection->TableFunction("pandas_scan", params)->Alias(name));
	rel->rel->extra_dependencies =
	    make_unique<PythonDependencies>(make_unique<RegisteredObject>(value), make_unique<RegisteredObject>(new_df));
	return rel;
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromCsvAuto(const string &filename) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	vector<Value> params;
	params.emplace_back(filename);
	return make_unique<DuckDBPyRelation>(connection->TableFunction("read_csv_auto", params)->Alias(filename));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromParquet(const string &filename, bool binary_as_string) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	vector<Value> params;
	params.emplace_back(filename);
	named_parameter_map_t named_parameters({{"binary_as_string", Value::BOOLEAN(binary_as_string)}});
	return make_unique<DuckDBPyRelation>(
	    connection->TableFunction("parquet_scan", params, named_parameters)->Alias(filename));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromArrow(py::object &arrow_object) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	py::gil_scoped_acquire acquire;
	string name = "arrow_object_" + GenerateRandomName();
	auto py_object_type = string(py::str(arrow_object.get_type().attr("__name__")));
	if (!IsAcceptedArrowObject(py_object_type)) {
		throw InvalidInputException("Python Object Type %s is not an accepted Arrow Object.", py_object_type);
	}
	auto stream_factory =
	    make_unique<PythonTableArrowArrayStreamFactory>(arrow_object.ptr(), connection->context->config);

	auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
	auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;

	auto rel = make_unique<DuckDBPyRelation>(
	    connection
	        ->TableFunction("arrow_scan", {Value::POINTER((uintptr_t)stream_factory.get()),
	                                       Value::POINTER((uintptr_t)stream_factory_produce),
	                                       Value::POINTER((uintptr_t)stream_factory_get_schema)})
	        ->Alias(name));
	rel->rel->extra_dependencies =
	    make_unique<PythonDependencies>(make_unique<RegisteredArrow>(move(stream_factory), arrow_object));
	return rel;
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromSubstrait(py::bytes &proto) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	string name = "substrait_" + GenerateRandomName();
	vector<Value> params;
	params.emplace_back(Value::BLOB_RAW(proto));
	return make_unique<DuckDBPyRelation>(connection->TableFunction("from_substrait", params)->Alias(name));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::GetSubstrait(const string &query) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	vector<Value> params;
	params.emplace_back(query);
	return make_unique<DuckDBPyRelation>(connection->TableFunction("get_substrait", params)->Alias(query));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::GetSubstraitJSON(const string &query) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	vector<Value> params;
	params.emplace_back(query);
	return make_unique<DuckDBPyRelation>(connection->TableFunction("get_substrait_json", params)->Alias(query));
}

unordered_set<string> DuckDBPyConnection::GetTableNames(const string &query) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	return connection->GetTableNames(query);
}

DuckDBPyConnection *DuckDBPyConnection::UnregisterPythonObject(const string &name) {
	connection->context->external_dependencies.erase(name);
	temporary_views.erase(name);
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

void DuckDBPyConnection::InstallExtension(const string &extension, bool force_install) {
	ExtensionHelper::InstallExtension(*connection->context, extension, force_install);
}

void DuckDBPyConnection::LoadExtension(const string &extension) {
	ExtensionHelper::LoadExternalExtension(*connection->context, extension);
}

// cursor() is stupid
shared_ptr<DuckDBPyConnection> DuckDBPyConnection::Cursor() {
	auto res = make_shared<DuckDBPyConnection>();
	res->database = database;
	res->connection = make_unique<Connection>(*res->database);
	cursors.push_back(res);
	return res;
}

// these should be functions on the result but well
py::object DuckDBPyConnection::FetchOne() {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->Fetchone();
}

py::list DuckDBPyConnection::FetchMany(idx_t size) {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->Fetchmany(size);
}

py::list DuckDBPyConnection::FetchAll() {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->Fetchall();
}

py::dict DuckDBPyConnection::FetchNumpy() {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchNumpyInternal();
}
DataFrame DuckDBPyConnection::FetchDF() {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchDF();
}

DataFrame DuckDBPyConnection::FetchDFChunk(const idx_t vectors_per_chunk) const {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchDFChunk(vectors_per_chunk);
}

duckdb::pyarrow::Table DuckDBPyConnection::FetchArrow(idx_t chunk_size) {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchArrowTable(chunk_size);
}

duckdb::pyarrow::RecordBatchReader DuckDBPyConnection::FetchRecordBatchReader(const idx_t chunk_size) const {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchRecordBatchReader(chunk_size);
}
static unique_ptr<TableFunctionRef> TryReplacement(py::dict &dict, py::str &table_name, ClientConfig &config,
                                                   py::object &current_frame) {
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
		auto new_df = PandasScanFunction::PandasReplaceCopiedNames(entry);
		children.push_back(make_unique<ConstantExpression>(Value::POINTER((uintptr_t)new_df.ptr())));
		table_function->function = make_unique<FunctionExpression>("pandas_scan", move(children));
		table_function->external_dependency = make_unique<PythonDependencies>(make_unique<RegisteredObject>(entry),
		                                                                      make_unique<RegisteredObject>(new_df));
	} else if (DuckDBPyConnection::IsAcceptedArrowObject(py_object_type)) {
		string name = "arrow_" + GenerateRandomName();
		auto stream_factory = make_unique<PythonTableArrowArrayStreamFactory>(entry.ptr(), config);
		auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
		auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;

		children.push_back(make_unique<ConstantExpression>(Value::POINTER((uintptr_t)stream_factory.get())));
		children.push_back(make_unique<ConstantExpression>(Value::POINTER((uintptr_t)stream_factory_produce)));
		children.push_back(make_unique<ConstantExpression>(Value::POINTER((uintptr_t)stream_factory_get_schema)));

		table_function->function = make_unique<FunctionExpression>("arrow_scan", move(children));
		table_function->external_dependency =
		    make_unique<PythonDependencies>(make_unique<RegisteredArrow>(move(stream_factory), entry));
	} else {
		std::string location = py::cast<py::str>(current_frame.attr("f_code").attr("co_filename"));
		location += ":";
		location += py::cast<py::str>(current_frame.attr("f_lineno"));
		std::string cpp_table_name = table_name;

		throw InvalidInputException(
		    "Python Object \"%s\" of type \"%s\" found on line \"%s\" not suitable for replacement scans.\nMake sure "
		    "that \"%s\" is either a pandas.DataFrame, or pyarrow Table, FileSystemDataset, InMemoryDataset, "
		    "RecordBatchReader, or Scanner",
		    cpp_table_name, py_object_type, location, cpp_table_name);
	}
	return table_function;
}

static unique_ptr<TableFunctionRef> ScanReplacement(ClientContext &context, const string &table_name,
                                                    ReplacementScanData *data) {
	py::gil_scoped_acquire acquire;
	auto py_table_name = py::str(table_name);
	// Here we do an exhaustive search on the frame lineage
	auto current_frame = py::module::import("inspect").attr("currentframe")();
	while (hasattr(current_frame, "f_locals")) {
		auto local_dict = py::reinterpret_borrow<py::dict>(current_frame.attr("f_locals"));
		// search local dictionary
		if (local_dict) {
			auto result = TryReplacement(local_dict, py_table_name, context.config, current_frame);
			if (result) {
				return result;
			}
		}
		// search global dictionary
		auto global_dict = py::reinterpret_borrow<py::dict>(current_frame.attr("f_globals"));
		if (global_dict) {
			auto result = TryReplacement(global_dict, py_table_name, context.config, current_frame);
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
                                                           py::object config_options) {
	auto res = make_shared<DuckDBPyConnection>();

	DBConfig config;

	if (config_options.is_none()) {
		config_options = py::dict();
	}
	if (!py::isinstance<py::dict>(config_options)) {
		throw InvalidInputException("Type of object passed to parameter 'config' has to be <dict>");
	}
	if (read_only) {
		config.options.access_mode = AccessMode::READ_ONLY;
	}
	py::dict config_dict = config_options;
	for (auto &kv : config_dict) {
		string key = py::str(kv.first);
		string val = py::str(kv.second);
		auto config_property = DBConfig::GetOptionByName(key);
		if (!config_property) {
			throw InvalidInputException("Unrecognized configuration property \"%s\"", key);
		}
		config.SetOption(*config_property, Value(val));
	}
	res->database = make_unique<DuckDB>(database, &config);
	res->connection = make_unique<Connection>(*res->database);
	if (config.options.enable_external_access) {
		res->database->instance->config.replacement_scans.emplace_back(ScanReplacement);
	}

	auto &db_config = res->database->instance->config;
	db_config.AddExtensionOption("pandas_analyze_sample",
	                             "The maximum number of rows to sample when analyzing a pandas object column.",
	                             LogicalType::UBIGINT);
	db_config.options.set_variables["pandas_analyze_sample"] = Value::UBIGINT(1000);

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

vector<Value> DuckDBPyConnection::TransformPythonParamList(py::handle params) {
	vector<Value> args;
	args.reserve(py::len(params));

	for (auto param : params) {
		args.emplace_back(TransformPythonValue(param, LogicalType::UNKNOWN, false));
	}
	return args;
}

DuckDBPyConnection *DuckDBPyConnection::DefaultConnection() {
	if (!default_connection) {
		py::dict config_dict;
		default_connection = DuckDBPyConnection::Connect(":memory:", false, config_dict);
	}
	return default_connection.get();
}

PythonImportCache *DuckDBPyConnection::ImportCache() {
	if (!import_cache) {
		import_cache = make_shared<PythonImportCache>();
	}
	return import_cache.get();
}

DuckDBPyConnection *DuckDBPyConnection::Enter() {
	return this;
}

bool DuckDBPyConnection::Exit(DuckDBPyConnection &self, const py::object &exc_type, const py::object &exc,
                              const py::object &traceback) {
	self.Close();
	return true;
}

void DuckDBPyConnection::Cleanup() {
	default_connection.reset();
	import_cache.reset();
}

bool DuckDBPyConnection::IsAcceptedArrowObject(string &py_object_type) {
	if (py_object_type == "Table" || py_object_type == "FileSystemDataset" || py_object_type == "InMemoryDataset" ||
	    py_object_type == "RecordBatchReader" || py_object_type == "Scanner") {
		return true;
	}
	return false;
}
unique_lock<std::mutex> DuckDBPyConnection::AcquireConnectionLock() {
	// we first release the gil and then acquire the connection lock
	unique_lock<std::mutex> lock(py_connection_lock, std::defer_lock);
	{
		py::gil_scoped_release release;
		lock.lock();
	}
	return lock;
}

} // namespace duckdb
