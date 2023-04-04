#include "duckdb_python/pyconnection/pyconnection.hpp"

#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/relation/read_csv_relation.hpp"
#include "duckdb/main/relation/read_json_relation.hpp"
#include "duckdb/main/relation/value_relation.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb_python/arrow/arrow_array_stream.hpp"
#include "duckdb_python/map.hpp"
#include "duckdb_python/pandas/pandas_scan.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/python_conversion.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb_python/jupyter_progress_bar_display.hpp"
#include "duckdb_python/pyfilesystem.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/main/relation/value_relation.hpp"
#include "duckdb_python/filesystem_object.hpp"

#include <random>

#include "duckdb/common/printer.hpp"

namespace duckdb {

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::default_connection = nullptr;
DBInstanceCache instance_cache;
shared_ptr<PythonImportCache> DuckDBPyConnection::import_cache = nullptr;
PythonEnvironmentType DuckDBPyConnection::environment = PythonEnvironmentType::NORMAL;

void DuckDBPyConnection::DetectEnvironment() {
	// If __main__ does not have a __file__ attribute, we are in interactive mode
	auto main_module = py::module_::import("__main__");
	if (py::hasattr(main_module, "__file__")) {
		return;
	}
	DuckDBPyConnection::environment = PythonEnvironmentType::INTERACTIVE;
	if (!ModuleIsLoaded<IPythonCacheItem>()) {
		return;
	}

	// Check to see if we are in a Jupyter Notebook
	auto &import_cache_py = *DuckDBPyConnection::ImportCache();
	auto get_ipython = import_cache_py.IPython().get_ipython();
	if (get_ipython.ptr() == nullptr) {
		// Could either not load the IPython module, or it has no 'get_ipython' attribute
		return;
	}
	auto ipython = get_ipython();
	if (!py::hasattr(ipython, "config")) {
		return;
	}
	py::dict ipython_config = ipython.attr("config");
	if (ipython_config.contains("IPKernelApp")) {
		DuckDBPyConnection::environment = PythonEnvironmentType::JUPYTER;
	}
	return;
}

bool DuckDBPyConnection::DetectAndGetEnvironment() {
	DuckDBPyConnection::DetectEnvironment();
	return DuckDBPyConnection::IsInteractive();
}

bool DuckDBPyConnection::IsJupyter() {
	return DuckDBPyConnection::environment == PythonEnvironmentType::JUPYTER;
}

static bool IsArrowBackedDataFrame(const py::object &df) {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	D_ASSERT(py::isinstance(df, import_cache.pandas().DataFrame()));

	py::list dtypes = df.attr("dtypes");
	if (dtypes.empty()) {
		return false;
	}
	// TODO: make this optional, will throw on pandas < 2.0.0
	auto arrow_dtype =
	    py::module_::import("pandas").attr("core").attr("arrays").attr("arrow").attr("dtype").attr("ArrowDtype");
	// Frankenstein dataframes are a thing, so we might have to deal with mixed pyarrow and numpy somewhere else
	for (auto &dtype : dtypes) {
		if (!py::isinstance(dtype, arrow_dtype)) {
			return false;
		}
	}
	return true;
}

py::object ArrowTableFromDataframe(const py::object &df) {
	// Construct a pyarrow.lib.Table from the internal arrays
	py::list names = df.attr("columns");
	auto getter = df.attr("__getitem__");
	py::list array_list;
	for (auto &name : names) {
		py::object column = getter(name);
		auto array = column.attr("array").attr("__arrow_array__")();
		array_list.append(array);
	}
	return py::module_::import("pyarrow").attr("lib").attr("Table").attr("from_arrays")(array_list,
	                                                                                    py::arg("names") = names);
}

static void InitializeConnectionMethods(py::class_<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>> &m) {
	m.def("cursor", &DuckDBPyConnection::Cursor, "Create a duplicate of the current connection")
	    .def("register_filesystem", &DuckDBPyConnection::RegisterFilesystem, "Register a fsspec compliant filesystem",
	         py::arg("filesystem"))
	    .def("unregister_filesystem", &DuckDBPyConnection::UnregisterFilesystem, "Unregister a filesystem",
	         py::arg("name"))
	    .def("list_filesystems", &DuckDBPyConnection::ListFilesystems,
	         "List registered filesystems, including builtin ones")
	    .def("filesystem_is_registered", &DuckDBPyConnection::FileSystemIsRegistered,
	         "Check if a filesystem with the provided name is currently registered", py::arg("name"))
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
	    .def("fetchdf", &DuckDBPyConnection::FetchDF, "Fetch a result as DataFrame following execute()", py::kw_only(),
	         py::arg("date_as_object") = false)
	    .def("fetch_df", &DuckDBPyConnection::FetchDF, "Fetch a result as DataFrame following execute()", py::kw_only(),
	         py::arg("date_as_object") = false)
	    .def("fetch_df_chunk", &DuckDBPyConnection::FetchDFChunk,
	         "Fetch a chunk of the result as Data.Frame following execute()", py::arg("vectors_per_chunk") = 1,
	         py::kw_only(), py::arg("date_as_object") = false)
	    .def("df", &DuckDBPyConnection::FetchDF, "Fetch a result as DataFrame following execute()", py::kw_only(),
	         py::arg("date_as_object") = false)
	    .def("pl", &DuckDBPyConnection::FetchPolars, "Fetch a result as Polars DataFrame following execute()",
	         py::arg("chunk_size") = 1000000)
	    .def("fetch_arrow_table", &DuckDBPyConnection::FetchArrow, "Fetch a result as Arrow table following execute()",
	         py::arg("chunk_size") = 1000000)
	    .def("fetch_record_batch", &DuckDBPyConnection::FetchRecordBatchReader,
	         "Fetch an Arrow RecordBatchReader following execute()", py::arg("chunk_size") = 1000000)
	    .def("arrow", &DuckDBPyConnection::FetchArrow, "Fetch a result as Arrow table following execute()",
	         py::arg("chunk_size") = 1000000)
	    .def("torch", &DuckDBPyConnection::FetchPyTorch,
	         "Fetch a result as dict of PyTorch Tensors following execute()")
	    .def("tf", &DuckDBPyConnection::FetchTF, "Fetch a result as dict of TensorFlow Tensors following execute()")
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
	    .def("read_json", &DuckDBPyConnection::ReadJSON, "Create a relation object from the JSON file in 'name'",
	         py::arg("name"), py::kw_only(), py::arg("columns") = py::none(), py::arg("sample_size") = py::none(),
	         py::arg("maximum_depth") = py::none());

	DefineMethod({"sql", "query", "from_query"}, m, &DuckDBPyConnection::RunQuery,
	             "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, "
	             "otherwise run the query as-is.",
	             py::arg("query"), py::arg("alias") = "query_relation");

	DefineMethod({"read_csv", "from_csv_auto"}, m, &DuckDBPyConnection::ReadCSV,
	             "Create a relation object from the CSV file in 'name'", py::arg("name"), py::kw_only(),
	             py::arg("header") = py::none(), py::arg("compression") = py::none(), py::arg("sep") = py::none(),
	             py::arg("delimiter") = py::none(), py::arg("dtype") = py::none(), py::arg("na_values") = py::none(),
	             py::arg("skiprows") = py::none(), py::arg("quotechar") = py::none(),
	             py::arg("escapechar") = py::none(), py::arg("encoding") = py::none(), py::arg("parallel") = py::none(),
	             py::arg("date_format") = py::none(), py::arg("timestamp_format") = py::none(),
	             py::arg("sample_size") = py::none(), py::arg("all_varchar") = py::none(),
	             py::arg("normalize_names") = py::none(), py::arg("filename") = py::none());

	m.def("from_df", &DuckDBPyConnection::FromDF, "Create a relation object from the Data.Frame in df",
	      py::arg("df") = py::none())
	    .def("from_arrow", &DuckDBPyConnection::FromArrow, "Create a relation object from an Arrow object",
	         py::arg("arrow_object"));

	DefineMethod({"from_parquet", "read_parquet"}, m, &DuckDBPyConnection::FromParquet,
	             "Create a relation object from the Parquet files in file_glob", py::arg("file_glob"),
	             py::arg("binary_as_string") = false, py::kw_only(), py::arg("file_row_number") = false,
	             py::arg("filename") = false, py::arg("hive_partitioning") = false, py::arg("union_by_name") = false,
	             py::arg("compression") = py::none());
	DefineMethod({"from_parquet", "read_parquet"}, m, &DuckDBPyConnection::FromParquets,
	             "Create a relation object from the Parquet files in file_globs", py::arg("file_globs"),
	             py::arg("binary_as_string") = false, py::kw_only(), py::arg("file_row_number") = false,
	             py::arg("filename") = false, py::arg("hive_partitioning") = false, py::arg("union_by_name") = false,
	             py::arg("compression") = py::none());

	m.def("from_substrait", &DuckDBPyConnection::FromSubstrait, "Create a query object from protobuf plan",
	      py::arg("proto"))
	    .def("get_substrait", &DuckDBPyConnection::GetSubstrait, "Serialize a query to protobuf", py::arg("query"),
	         py::kw_only(), py::arg("enable_optimizer") = true)
	    .def("get_substrait_json", &DuckDBPyConnection::GetSubstraitJSON,
	         "Serialize a query to protobuf on the JSON format", py::arg("query"), py::kw_only(),
	         py::arg("enable_optimizer") = true)
	    .def("from_substrait_json", &DuckDBPyConnection::FromSubstraitJSON,
	         "Create a query object from a JSON protobuf plan", py::arg("json"))
	    .def("get_table_names", &DuckDBPyConnection::GetTableNames, "Extract the required table names from a query",
	         py::arg("query"))
	    .def_property_readonly("description", &DuckDBPyConnection::GetDescription,
	                           "Get result set attributes, mainly column names")
	    .def("install_extension", &DuckDBPyConnection::InstallExtension, "Install an extension by name",
	         py::arg("extension"), py::kw_only(), py::arg("force_install") = false)
	    .def("load_extension", &DuckDBPyConnection::LoadExtension, "Load an installed extension", py::arg("extension"));
}

void DuckDBPyConnection::UnregisterFilesystem(const py::str &name) {
	auto &fs = database->GetFileSystem();

	fs.UnregisterSubSystem(name);
}

void DuckDBPyConnection::RegisterFilesystem(AbstractFileSystem filesystem) {
	PythonGILWrapper gil_wrapper;

	if (!py::isinstance<AbstractFileSystem>(filesystem)) {
		throw InvalidInputException("Bad filesystem instance");
	}

	auto &fs = database->GetFileSystem();

	auto protocol = filesystem.attr("protocol");
	if (protocol.is_none() || py::str("abstract").equal(protocol)) {
		throw InvalidInputException("Must provide concrete fsspec implementation");
	}

	vector<string> protocols;
	if (py::isinstance<py::str>(protocol)) {
		protocols.push_back(py::str(protocol));
	} else {
		for (const auto &sub_protocol : protocol) {
			protocols.push_back(py::str(sub_protocol));
		}
	}

	fs.RegisterSubSystem(make_uniq<PythonFilesystem>(std::move(protocols), std::move(filesystem)));
}

py::list DuckDBPyConnection::ListFilesystems() {
	auto subsystems = database->GetFileSystem().ListSubSystems();
	py::list names;
	for (auto &name : subsystems) {
		names.append(py::str(name));
	}
	return names;
}

bool DuckDBPyConnection::FileSystemIsRegistered(const string &name) {
	auto subsystems = database->GetFileSystem().ListSubSystems();
	return std::find(subsystems.begin(), subsystems.end(), name) != subsystems.end();
}

void DuckDBPyConnection::Initialize(py::handle &m) {
	auto connection_module =
	    py::class_<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>>(m, "DuckDBPyConnection", py::module_local());

	connection_module.def("__enter__", &DuckDBPyConnection::Enter)
	    .def("__exit__", &DuckDBPyConnection::Exit, py::arg("exc_type"), py::arg("exc"), py::arg("traceback"));

	InitializeConnectionMethods(connection_module);
	PyDateTime_IMPORT;
	DuckDBPyConnection::ImportCache();
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::ExecuteMany(const string &query, py::object params) {
	if (params.is_none()) {
		params = py::list();
	}
	Execute(query, std::move(params), true);
	return shared_from_this();
}

unique_ptr<QueryResult> DuckDBPyConnection::CompletePendingQuery(PendingQueryResult &pending_query) {
	PendingExecutionResult execution_result;
	do {
		execution_result = pending_query.ExecuteTask();
		{
			py::gil_scoped_acquire gil;
			if (PyErr_CheckSignals() != 0) {
				throw std::runtime_error("Query interrupted");
			}
		}
	} while (execution_result == PendingExecutionResult::RESULT_NOT_READY);
	if (execution_result == PendingExecutionResult::EXECUTION_ERROR) {
		pending_query.ThrowError();
	}
	return pending_query.Execute();
}

py::list TransformNamedParameters(const case_insensitive_map_t<idx_t> &named_param_map, const py::dict &params) {
	py::list new_params(params.size());

	for (auto &item : params) {
		const std::string &item_name = item.first.cast<std::string>();
		auto entry = named_param_map.find(item_name);
		if (entry == named_param_map.end()) {
			throw InvalidInputException(
			    "Named parameters could not be transformed, because query string is missing named parameter '%s'",
			    item_name);
		}
		auto param_idx = entry->second;
		// Add the value of the named parameter to the list
		new_params[param_idx - 1] = item.second;
	}

	if (named_param_map.size() != params.size()) {
		// One or more named parameters were expected, but not found
		vector<string> missing_params;
		missing_params.reserve(named_param_map.size());
		for (auto &entry : named_param_map) {
			auto &name = entry.first;
			if (!params.contains(name)) {
				missing_params.push_back(name);
			}
		}
		auto message = StringUtil::Join(missing_params, ", ");
		throw InvalidInputException("Not all named parameters have been located, missing: %s", message);
	}

	return new_params;
}

unique_ptr<QueryResult> DuckDBPyConnection::ExecuteInternal(const string &query, py::object params, bool many) {
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
			return nullptr;
		}
		// if there are multiple statements, we directly execute the statements besides the last one
		// we only return the result of the last statement to the user, unless one of the previous statements fails
		for (idx_t i = 0; i + 1 < statements.size(); i++) {
			auto pending_query = connection->PendingQuery(std::move(statements[i]), false);
			auto res = CompletePendingQuery(*pending_query);

			if (res->HasError()) {
				res->ThrowError();
			}
		}

		prep = connection->Prepare(std::move(statements.back()));
		if (prep->HasError()) {
			prep->error.Throw();
		}
	}

	auto &named_param_map = prep->named_param_map;
	if (py::isinstance<py::dict>(params)) {
		if (named_param_map.empty()) {
			throw InvalidInputException("Param is of type 'dict', but no named parameters were found in the query");
		}
		// Transform named parameters to regular positional parameters
		params = TransformNamedParameters(named_param_map, params);
		// Clear the map, we don't need it anymore
		prep->named_param_map.clear();
	} else if (!named_param_map.empty()) {
		throw InvalidInputException("Named parameters found, but param is not of type 'dict'");
	}

	// this is a list of a list of parameters in executemany
	py::list params_set;
	if (!many) {
		params_set = py::list(1);
		params_set[0] = params;
	} else {
		params_set = params;
	}

	// For every entry of the argument list, execute the prepared statement with said arguments
	for (pybind11::handle single_query_params : params_set) {
		if (prep->n_param != py::len(single_query_params)) {
			throw InvalidInputException("Prepared statement needs %d parameters, %d given", prep->n_param,
			                            py::len(single_query_params));
		}
		auto args = DuckDBPyConnection::TransformPythonParamList(single_query_params);
		unique_ptr<QueryResult> res;
		{
			py::gil_scoped_release release;
			unique_lock<std::mutex> lock(py_connection_lock);
			auto pending_query = prep->PendingQuery(args);
			res = CompletePendingQuery(*pending_query);

			if (res->HasError()) {
				res->ThrowError();
			}
		}

		if (!many) {
			return res;
		}
	}
	return nullptr;
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::Execute(const string &query, py::object params, bool many) {
	auto res = ExecuteInternal(query, std::move(params), many);
	if (res) {
		auto py_result = make_uniq<DuckDBPyResult>(std::move(res));
		result = make_uniq<DuckDBPyRelation>(std::move(py_result));
	}
	return shared_from_this();
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::Append(const string &name, const DataFrame &value) {
	RegisterPythonObject("__append_df", value);
	return Execute("INSERT INTO \"" + name + "\" SELECT * FROM __append_df");
}

void DuckDBPyConnection::RegisterArrowObject(const py::object &arrow_object, const string &name) {
	auto stream_factory =
	    make_uniq<PythonTableArrowArrayStreamFactory>(arrow_object.ptr(), connection->context->config);
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
	    make_shared<PythonDependencies>(make_uniq<RegisteredArrow>(std::move(stream_factory), arrow_object)));
	connection->context->external_dependencies[name] = std::move(dependencies);
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::RegisterPythonObject(const string &name,
                                                                        const py::object &python_object) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}

	if (DuckDBPyConnection::IsPandasDataframe(python_object)) {
		if (IsArrowBackedDataFrame(python_object)) {
			auto arrow_table = ArrowTableFromDataframe(python_object);
			RegisterArrowObject(arrow_table, name);
		} else {
			auto new_df = PandasScanFunction::PandasReplaceCopiedNames(python_object);
			{
				py::gil_scoped_release release;
				temporary_views[name] =
				    connection->TableFunction("pandas_scan", {Value::POINTER((uintptr_t)new_df.ptr())})
				        ->CreateView(name, true, true);
			}

			// keep a reference
			vector<shared_ptr<ExternalDependency>> dependencies;
			dependencies.push_back(make_shared<PythonDependencies>(make_uniq<RegisteredObject>(python_object),
			                                                       make_uniq<RegisteredObject>(new_df)));
			connection->context->external_dependencies[name] = std::move(dependencies);
		}
	} else if (IsAcceptedArrowObject(python_object) || IsPolarsDataframe(python_object)) {
		py::object arrow_object;
		if (IsPolarsDataframe(python_object)) {
			if (PolarsDataFrame::IsDataFrame(python_object)) {
				arrow_object = python_object.attr("to_arrow")();
			} else if (PolarsDataFrame::IsLazyFrame(python_object)) {
				py::object materialized = python_object.attr("collect")();
				arrow_object = materialized.attr("to_arrow")();
			} else {
				throw NotImplementedException("Unsupported Polars DF Type");
			}
		} else {
			arrow_object = python_object;
		}
		RegisterArrowObject(arrow_object, name);
	} else if (DuckDBPyRelation::IsRelation(python_object)) {
		auto pyrel = py::cast<DuckDBPyRelation *>(python_object);
		pyrel->CreateView(name, true);
	} else {
		auto py_object_type = string(py::str(python_object.get_type().attr("__name__")));
		throw InvalidInputException("Python Object %s not suitable to be registered as a view", py_object_type);
	}
	return shared_from_this();
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::ReadJSON(const string &name, const py::object &columns,
                                                          const py::object &sample_size,
                                                          const py::object &maximum_depth) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}

	named_parameter_map_t options;

	if (!py::none().is(columns)) {
		if (!py::isinstance<py::dict>(columns)) {
			throw InvalidInputException("read_json only accepts 'columns' as a dict[str, str]");
		}
		py::dict columns_dict = columns;
		child_list_t<Value> struct_fields;

		for (auto &kv : columns_dict) {
			auto &column_name = kv.first;
			auto &type = kv.second;
			if (!py::isinstance<py::str>(column_name)) {
				string actual_type = py::str(column_name.get_type());
				throw InvalidInputException("The provided column name must be a str, not of type '%s'", actual_type);
			}
			if (!py::isinstance<py::str>(type)) {
				string actual_type = py::str(column_name.get_type());
				throw InvalidInputException("The provided column type must be a str, not of type '%s'", actual_type);
			}
			struct_fields.emplace_back(py::str(column_name), Value(py::str(type)));
		}
		auto dtype_struct = Value::STRUCT(std::move(struct_fields));
		options["columns"] = std::move(dtype_struct);
	}

	if (!py::none().is(sample_size)) {
		if (!py::isinstance<py::int_>(sample_size)) {
			string actual_type = py::str(sample_size.get_type());
			throw InvalidInputException("read_json only accepts 'sample_size' as an integer, not '%s'", actual_type);
		}
		options["sample_size"] = Value::INTEGER(py::int_(sample_size));
	}

	if (!py::none().is(maximum_depth)) {
		if (!py::isinstance<py::int_>(maximum_depth)) {
			string actual_type = py::str(maximum_depth.get_type());
			throw InvalidInputException("read_json only accepts 'maximum_depth' as an integer, not '%s'", actual_type);
		}
		options["maximum_depth"] = Value::INTEGER(py::int_(maximum_depth));
	}

	bool auto_detect = false;
	if (!options.count("columns")) {
		options["auto_detect"] = Value::BOOLEAN(true);
		auto_detect = true;
	}

	auto read_json_relation = make_shared<ReadJSONRelation>(connection->context, name, std::move(options), auto_detect);
	if (read_json_relation == nullptr) {
		throw InvalidInputException("read_json can only be used when the JSON extension is (statically) loaded");
	}
	return make_uniq<DuckDBPyRelation>(std::move(read_json_relation));
}

PathLike DuckDBPyConnection::GetPathLike(const py::object &object) {
	return PathLike::Create(object, *this);
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::ReadCSV(
    const py::object &name_p, const py::object &header, const py::object &compression, const py::object &sep,
    const py::object &delimiter, const py::object &dtype, const py::object &na_values, const py::object &skiprows,
    const py::object &quotechar, const py::object &escapechar, const py::object &encoding, const py::object &parallel,
    const py::object &date_format, const py::object &timestamp_format, const py::object &sample_size,
    const py::object &all_varchar, const py::object &normalize_names, const py::object &filename) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	BufferedCSVReaderOptions options;
	auto path_like = GetPathLike(name_p);
	auto &name = path_like.str;
	auto file_like_object_wrapper = std::move(path_like.dependency);

	// First check if the header is explicitly set
	// when false this affects the returned types, so it needs to be known at initialization of the relation
	if (!py::none().is(header)) {

		bool header_as_int = py::isinstance<py::int_>(header);
		bool header_as_bool = py::isinstance<py::bool_>(header);

		if (header_as_bool) {
			options.SetHeader(py::bool_(header));
		} else if (header_as_int) {
			if ((int)py::int_(header) != 0) {
				throw InvalidInputException("read_csv only accepts 0 if 'header' is given as an integer");
			}
			options.SetHeader(true);
		} else {
			throw InvalidInputException("read_csv only accepts 'header' as an integer, or a boolean");
		}
	}

	// We want to detect if the file can be opened, we set this in the options so we can detect this at bind time
	// rather than only at execution time
	if (!py::none().is(compression)) {
		if (!py::isinstance<py::str>(compression)) {
			throw InvalidInputException("read_csv only accepts 'compression' as a string");
		}
		options.SetCompression(py::str(compression));
	}

	auto read_csv_p = connection->ReadCSV(name, options);
	auto &read_csv = (ReadCSVRelation &)*read_csv_p;
	if (file_like_object_wrapper) {
		D_ASSERT(!read_csv.extra_dependencies);
		read_csv.extra_dependencies = std::move(file_like_object_wrapper);
	}

	if (options.has_header) {
		// 'options' is only used to initialize the ReadCSV relation
		// we also need to set this in the arguments passed to the function
		read_csv.AddNamedParameter("header", Value::BOOLEAN(options.header));
	}

	if (options.compression != FileCompressionType::AUTO_DETECT) {
		read_csv.AddNamedParameter("compression", Value(py::str(compression)));
	}

	bool has_sep = !py::none().is(sep);
	bool has_delimiter = !py::none().is(delimiter);
	if (has_sep && has_delimiter) {
		throw InvalidInputException("read_csv takes either 'delimiter' or 'sep', not both");
	}
	if (has_sep) {
		read_csv.AddNamedParameter("delim", Value(py::str(sep)));
	} else if (has_delimiter) {
		read_csv.AddNamedParameter("delim", Value(py::str(delimiter)));
	}

	// We don't support overriding the names of the header yet
	// 'names'
	// if (keywords.count("names")) {
	//	if (!py::isinstance<py::list>(kwargs["names"])) {
	//		throw InvalidInputException("read_csv only accepts 'names' as a list of strings");
	//	}
	//	vector<string> names;
	//	py::list names_list = kwargs["names"];
	//	for (auto& elem : names_list) {
	//		if (!py::isinstance<py::str>(elem)) {
	//			throw InvalidInputException("read_csv 'names' list has to consist of only strings");
	//		}
	//		names.push_back(py::str(elem));
	//	}
	//	// FIXME: Check for uniqueness of 'names' ?
	//}

	if (!py::none().is(dtype)) {
		if (py::isinstance<py::dict>(dtype)) {
			child_list_t<Value> struct_fields;
			py::dict dtype_dict = dtype;
			for (auto &kv : dtype_dict) {
				struct_fields.emplace_back(py::str(kv.first), Value(py::str(kv.second)));
			}
			auto dtype_struct = Value::STRUCT(std::move(struct_fields));
			read_csv.AddNamedParameter("dtypes", std::move(dtype_struct));
		} else if (py::isinstance<py::list>(dtype)) {
			auto dtype_list = TransformPythonValue(py::list(dtype));
			D_ASSERT(dtype_list.type().id() == LogicalTypeId::LIST);
			auto &children = ListValue::GetChildren(dtype_list);
			for (auto &child : children) {
				if (child.type().id() != LogicalTypeId::VARCHAR) {
					throw InvalidInputException("The types provided to 'dtype' have to be strings");
				}
			}
			read_csv.AddNamedParameter("dtypes", std::move(dtype_list));
		} else {
			throw InvalidInputException("read_csv only accepts 'dtype' as a dictionary or a list of strings");
		}
	}

	if (!py::none().is(na_values)) {
		if (!py::isinstance<py::str>(na_values)) {
			throw InvalidInputException("read_csv only accepts 'na_values' as a string");
		}
		read_csv.AddNamedParameter("nullstr", Value(py::str(na_values)));
	}

	if (!py::none().is(skiprows)) {
		if (!py::isinstance<py::int_>(skiprows)) {
			throw InvalidInputException("read_csv only accepts 'skiprows' as an integer");
		}
		read_csv.AddNamedParameter("skip", Value::INTEGER(py::int_(skiprows)));
	}

	if (!py::none().is(parallel)) {
		if (!py::isinstance<py::bool_>(parallel)) {
			throw InvalidInputException("read_csv only accepts 'parallel' as a boolean");
		}
		read_csv.AddNamedParameter("parallel", Value::BOOLEAN(py::bool_(parallel)));
	}

	if (!py::none().is(quotechar)) {
		if (!py::isinstance<py::str>(quotechar)) {
			throw InvalidInputException("read_csv only accepts 'quotechar' as a string");
		}
		read_csv.AddNamedParameter("quote", Value(py::str(quotechar)));
	}

	if (!py::none().is(escapechar)) {
		if (!py::isinstance<py::str>(escapechar)) {
			throw InvalidInputException("read_csv only accepts 'escapechar' as a string");
		}
		read_csv.AddNamedParameter("escape", Value(py::str(escapechar)));
	}

	if (!py::none().is(encoding)) {
		if (!py::isinstance<py::str>(encoding)) {
			throw InvalidInputException("read_csv only accepts 'encoding' as a string");
		}
		string encoding_str = StringUtil::Lower(py::str(encoding));
		if (encoding_str != "utf8" && encoding_str != "utf-8") {
			throw BinderException("Copy is only supported for UTF-8 encoded files, ENCODING 'UTF-8'");
		}
	}

	if (!py::none().is(date_format)) {
		if (!py::isinstance<py::str>(date_format)) {
			throw InvalidInputException("read_csv only accepts 'date_format' as a string");
		}
		read_csv.AddNamedParameter("dateformat", Value(py::str(date_format)));
	}

	if (!py::none().is(timestamp_format)) {
		if (!py::isinstance<py::str>(timestamp_format)) {
			throw InvalidInputException("read_csv only accepts 'timestamp_format' as a string");
		}
		read_csv.AddNamedParameter("timestampformat", Value(py::str(timestamp_format)));
	}

	if (!py::none().is(sample_size)) {
		if (!py::isinstance<py::int_>(sample_size)) {
			throw InvalidInputException("read_csv only accepts 'sample_size' as an integer");
		}
		read_csv.AddNamedParameter("sample_size", Value::INTEGER(py::int_(sample_size)));
	}

	if (!py::none().is(all_varchar)) {
		if (!py::isinstance<py::bool_>(all_varchar)) {
			throw InvalidInputException("read_csv only accepts 'all_varchar' as a boolean");
		}
		read_csv.AddNamedParameter("all_varchar", Value::INTEGER(py::bool_(all_varchar)));
	}

	if (!py::none().is(normalize_names)) {
		if (!py::isinstance<py::bool_>(normalize_names)) {
			throw InvalidInputException("read_csv only accepts 'normalize_names' as a boolean");
		}
		read_csv.AddNamedParameter("normalize_names", Value::INTEGER(py::bool_(normalize_names)));
	}

	if (!py::none().is(filename)) {
		if (!py::isinstance<py::bool_>(filename)) {
			throw InvalidInputException("read_csv only accepts 'filename' as a boolean");
		}
		read_csv.AddNamedParameter("filename", Value::INTEGER(py::bool_(filename)));
	}

	return make_uniq<DuckDBPyRelation>(read_csv_p->Alias(name));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromQuery(const string &query, const string &alias) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	const char *duckdb_query_error = R"(duckdb.from_query cannot be used to run arbitrary SQL queries.
It can only be used to run individual SELECT statements, and converts the result of that SELECT
statement into a Relation object.
Use duckdb.sql to run arbitrary SQL queries.)";
	return make_uniq<DuckDBPyRelation>(connection->RelationFromQuery(query, alias, duckdb_query_error));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::RunQuery(const string &query, const string &alias) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	Parser parser(connection->context->GetParserOptions());
	parser.ParseQuery(query);
	if (parser.statements.size() == 1 && parser.statements[0]->type == StatementType::SELECT_STATEMENT) {
		return make_uniq<DuckDBPyRelation>(connection->RelationFromQuery(
		    unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0])), alias));
	}
	auto res = ExecuteInternal(query);
	if (!res) {
		return nullptr;
	}
	if (res->properties.return_type != StatementReturnType::QUERY_RESULT) {
		return nullptr;
	}
	// FIXME: we should add support for a relation object over a column data collection to make this more efficient
	vector<vector<Value>> values;
	vector<string> names = res->names;
	while (true) {
		auto chunk = res->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t r = 0; r < chunk->size(); r++) {
			vector<Value> row;
			for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
				row.push_back(chunk->data[c].GetValue(r));
			}
			values.push_back(std::move(row));
		}
	}
	if (values.empty()) {
		return nullptr;
	}
	return make_uniq<DuckDBPyRelation>(make_uniq<ValueRelation>(connection->context, values, names));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::Table(const string &tname) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	auto qualified_name = QualifiedName::Parse(tname);
	if (qualified_name.schema.empty()) {
		qualified_name.schema = DEFAULT_SCHEMA;
	}
	return make_uniq<DuckDBPyRelation>(connection->Table(qualified_name.schema, qualified_name.name));
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
	vector<vector<Value>> values {DuckDBPyConnection::TransformPythonParamList(params)};
	return make_uniq<DuckDBPyRelation>(connection->Values(values));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::View(const string &vname) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	// First check our temporary view
	if (temporary_views.find(vname) != temporary_views.end()) {
		return make_uniq<DuckDBPyRelation>(temporary_views[vname]);
	}
	return make_uniq<DuckDBPyRelation>(connection->View(vname));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::TableFunction(const string &fname, py::object params) {
	if (params.is_none()) {
		params = py::list();
	}
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}

	return make_uniq<DuckDBPyRelation>(
	    connection->TableFunction(fname, DuckDBPyConnection::TransformPythonParamList(params)));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromDF(const DataFrame &value) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	string name = "df_" + StringUtil::GenerateRandomName();
	if (IsArrowBackedDataFrame(value)) {
		auto table = ArrowTableFromDataframe(value);
		return DuckDBPyConnection::FromArrow(table);
	}
	auto new_df = PandasScanFunction::PandasReplaceCopiedNames(value);
	vector<Value> params;
	params.emplace_back(Value::POINTER((uintptr_t)new_df.ptr()));
	auto rel = connection->TableFunction("pandas_scan", params)->Alias(name);
	rel->extra_dependencies =
	    make_uniq<PythonDependencies>(make_uniq<RegisteredObject>(value), make_uniq<RegisteredObject>(new_df));
	return make_uniq<DuckDBPyRelation>(std::move(rel));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromParquet(const string &file_glob, bool binary_as_string,
                                                             bool file_row_number, bool filename,
                                                             bool hive_partitioning, bool union_by_name,
                                                             const py::object &compression) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	string name = "parquet_" + StringUtil::GenerateRandomName();
	vector<Value> params;
	params.emplace_back(file_glob);
	named_parameter_map_t named_parameters({{"binary_as_string", Value::BOOLEAN(binary_as_string)},
	                                        {"file_row_number", Value::BOOLEAN(file_row_number)},
	                                        {"filename", Value::BOOLEAN(filename)},
	                                        {"hive_partitioning", Value::BOOLEAN(hive_partitioning)},
	                                        {"union_by_name", Value::BOOLEAN(union_by_name)}});

	if (!py::none().is(compression)) {
		if (!py::isinstance<py::str>(compression)) {
			throw InvalidInputException("from_parquet only accepts 'compression' as a string");
		}
		named_parameters["compression"] = Value(py::str(compression));
	}
	return make_uniq<DuckDBPyRelation>(
	    connection->TableFunction("parquet_scan", params, named_parameters)->Alias(name));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromParquets(const vector<string> &file_globs, bool binary_as_string,
                                                              bool file_row_number, bool filename,
                                                              bool hive_partitioning, bool union_by_name,
                                                              const py::object &compression) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	string name = "parquet_" + StringUtil::GenerateRandomName();
	vector<Value> params;
	auto file_globs_as_value = vector<Value>();
	for (const auto &file : file_globs) {
		file_globs_as_value.emplace_back(file);
	}
	params.emplace_back(Value::LIST(file_globs_as_value));
	named_parameter_map_t named_parameters({{"binary_as_string", Value::BOOLEAN(binary_as_string)},
	                                        {"file_row_number", Value::BOOLEAN(file_row_number)},
	                                        {"filename", Value::BOOLEAN(filename)},
	                                        {"hive_partitioning", Value::BOOLEAN(hive_partitioning)},
	                                        {"union_by_name", Value::BOOLEAN(union_by_name)}});

	if (!py::none().is(compression)) {
		if (!py::isinstance<py::str>(compression)) {
			throw InvalidInputException("from_parquet only accepts 'compression' as a string");
		}
		named_parameters["compression"] = Value(py::str(compression));
	}

	return make_uniq<DuckDBPyRelation>(
	    connection->TableFunction("parquet_scan", params, named_parameters)->Alias(name));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromArrow(py::object &arrow_object) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	py::gil_scoped_acquire acquire;
	string name = "arrow_object_" + StringUtil::GenerateRandomName();
	if (!IsAcceptedArrowObject(arrow_object)) {
		auto py_object_type = string(py::str(arrow_object.get_type().attr("__name__")));
		throw InvalidInputException("Python Object Type %s is not an accepted Arrow Object.", py_object_type);
	}
	auto stream_factory =
	    make_uniq<PythonTableArrowArrayStreamFactory>(arrow_object.ptr(), connection->context->config);

	auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
	auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;

	auto rel = connection
	               ->TableFunction("arrow_scan", {Value::POINTER((uintptr_t)stream_factory.get()),
	                                              Value::POINTER((uintptr_t)stream_factory_produce),
	                                              Value::POINTER((uintptr_t)stream_factory_get_schema)})
	               ->Alias(name);
	rel->extra_dependencies =
	    make_uniq<PythonDependencies>(make_uniq<RegisteredArrow>(std::move(stream_factory), arrow_object));
	return make_uniq<DuckDBPyRelation>(std::move(rel));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromSubstrait(py::bytes &proto) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	string name = "substrait_" + StringUtil::GenerateRandomName();
	vector<Value> params;
	params.emplace_back(Value::BLOB_RAW(proto));
	return make_uniq<DuckDBPyRelation>(connection->TableFunction("from_substrait", params)->Alias(name));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::GetSubstrait(const string &query, bool enable_optimizer) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	vector<Value> params;
	params.emplace_back(query);
	named_parameter_map_t named_parameters({{"enable_optimizer", Value::BOOLEAN(enable_optimizer)}});
	return make_uniq<DuckDBPyRelation>(
	    connection->TableFunction("get_substrait", params, named_parameters)->Alias(query));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::GetSubstraitJSON(const string &query, bool enable_optimizer) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	vector<Value> params;
	params.emplace_back(query);
	named_parameter_map_t named_parameters({{"enable_optimizer", Value::BOOLEAN(enable_optimizer)}});
	return make_uniq<DuckDBPyRelation>(
	    connection->TableFunction("get_substrait_json", params, named_parameters)->Alias(query));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromSubstraitJSON(const string &json) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	string name = "from_substrait_" + StringUtil::GenerateRandomName();
	vector<Value> params;
	params.emplace_back(json);
	return make_uniq<DuckDBPyRelation>(connection->TableFunction("from_substrait_json", params)->Alias(name));
}

unordered_set<string> DuckDBPyConnection::GetTableNames(const string &query) {
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	return connection->GetTableNames(query);
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::UnregisterPythonObject(const string &name) {
	connection->context->external_dependencies.erase(name);
	temporary_views.erase(name);
	py::gil_scoped_release release;
	if (connection) {
		connection->Query("DROP VIEW \"" + name + "\"");
	}
	return shared_from_this();
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::Begin() {
	Execute("BEGIN TRANSACTION");
	return shared_from_this();
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::Commit() {
	if (connection->context->transaction.IsAutoCommit()) {
		return shared_from_this();
	}
	Execute("COMMIT");
	return shared_from_this();
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::Rollback() {
	Execute("ROLLBACK");
	return shared_from_this();
}

Optional<py::list> DuckDBPyConnection::GetDescription() {
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
	if (!connection) {
		throw ConnectionException("Connection has already been closed");
	}
	auto res = make_shared<DuckDBPyConnection>();
	res->database = database;
	res->connection = make_uniq<Connection>(*res->database);
	cursors.push_back(res);
	return res;
}

// these should be functions on the result but well
Optional<py::tuple> DuckDBPyConnection::FetchOne() {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchOne();
}

py::list DuckDBPyConnection::FetchMany(idx_t size) {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchMany(size);
}

py::list DuckDBPyConnection::FetchAll() {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchAll();
}

py::dict DuckDBPyConnection::FetchNumpy() {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchNumpyInternal();
}

DataFrame DuckDBPyConnection::FetchDF(bool date_as_object) {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchDF(date_as_object);
}

DataFrame DuckDBPyConnection::FetchDFChunk(const idx_t vectors_per_chunk, bool date_as_object) const {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchDFChunk(vectors_per_chunk, date_as_object);
}

duckdb::pyarrow::Table DuckDBPyConnection::FetchArrow(idx_t chunk_size) {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->ToArrowTable(chunk_size);
}

py::dict DuckDBPyConnection::FetchPyTorch() {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchPyTorch();
}

py::dict DuckDBPyConnection::FetchTF() {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchTF();
}

PolarsDataFrame DuckDBPyConnection::FetchPolars(idx_t chunk_size) {
	auto arrow = FetchArrow(chunk_size);
	return py::cast<PolarsDataFrame>(py::module::import("polars").attr("DataFrame")(arrow));
}

duckdb::pyarrow::RecordBatchReader DuckDBPyConnection::FetchRecordBatchReader(const idx_t chunk_size) const {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
	return result->FetchRecordBatchReader(chunk_size);
}

static void CreateArrowScan(py::object entry, TableFunctionRef &table_function,
                            vector<unique_ptr<ParsedExpression>> &children, ClientConfig &config) {
	string name = "arrow_" + StringUtil::GenerateRandomName();
	auto stream_factory = make_uniq<PythonTableArrowArrayStreamFactory>(entry.ptr(), config);
	auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
	auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;

	children.push_back(make_uniq<ConstantExpression>(Value::POINTER((uintptr_t)stream_factory.get())));
	children.push_back(make_uniq<ConstantExpression>(Value::POINTER((uintptr_t)stream_factory_produce)));
	children.push_back(make_uniq<ConstantExpression>(Value::POINTER((uintptr_t)stream_factory_get_schema)));

	table_function.function = make_uniq<FunctionExpression>("arrow_scan", std::move(children));
	table_function.external_dependency =
	    make_uniq<PythonDependencies>(make_uniq<RegisteredArrow>(std::move(stream_factory), entry));
}

static unique_ptr<TableRef> TryReplacement(py::dict &dict, py::str &table_name, ClientConfig &config,
                                           py::object &current_frame) {
	if (!dict.contains(table_name)) {
		// not present in the globals
		return nullptr;
	}
	auto entry = dict[table_name];
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	if (DuckDBPyConnection::IsPandasDataframe(entry)) {
		if (IsArrowBackedDataFrame(entry)) {
			auto table = ArrowTableFromDataframe(entry);
			CreateArrowScan(table, *table_function, children, config);
		} else {
			string name = "df_" + StringUtil::GenerateRandomName();
			auto new_df = PandasScanFunction::PandasReplaceCopiedNames(entry);
			children.push_back(make_uniq<ConstantExpression>(Value::POINTER((uintptr_t)new_df.ptr())));
			table_function->function = make_uniq<FunctionExpression>("pandas_scan", std::move(children));
			table_function->external_dependency =
			    make_uniq<PythonDependencies>(make_uniq<RegisteredObject>(entry), make_uniq<RegisteredObject>(new_df));
		}

	} else if (DuckDBPyConnection::IsAcceptedArrowObject(entry)) {
		CreateArrowScan(entry, *table_function, children, config);
	} else if (DuckDBPyRelation::IsRelation(entry)) {
		auto pyrel = py::cast<DuckDBPyRelation *>(entry);
		// create a subquery from the underlying relation object
		auto select = make_uniq<SelectStatement>();
		select->node = pyrel->GetRel().GetQueryNode();

		auto subquery = make_uniq<SubqueryRef>(std::move(select));
		return std::move(subquery);
	} else if (PolarsDataFrame::IsDataFrame(entry)) {
		auto arrow_dataset = entry.attr("to_arrow")();
		CreateArrowScan(arrow_dataset, *table_function, children, config);
	} else if (PolarsDataFrame::IsLazyFrame(entry)) {
		auto materialized = entry.attr("collect")();
		auto arrow_dataset = materialized.attr("to_arrow")();
		CreateArrowScan(arrow_dataset, *table_function, children, config);
	} else {
		std::string location = py::cast<py::str>(current_frame.attr("f_code").attr("co_filename"));
		location += ":";
		location += py::cast<py::str>(current_frame.attr("f_lineno"));
		std::string cpp_table_name = table_name;
		auto py_object_type = string(py::str(entry.get_type().attr("__name__")));

		throw InvalidInputException(
		    "Python Object \"%s\" of type \"%s\" found on line \"%s\" not suitable for replacement scans.\nMake sure "
		    "that \"%s\" is either a pandas.DataFrame, duckdb.DuckDBPyRelation, pyarrow Table, Dataset, "
		    "RecordBatchReader, or Scanner",
		    cpp_table_name, py_object_type, location, cpp_table_name);
	}
	return std::move(table_function);
}

static unique_ptr<TableRef> ScanReplacement(ClientContext &context, const string &table_name,
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

unordered_map<string, string> TransformPyConfigDict(const py::dict &py_config_dict) {
	unordered_map<string, string> config_dict;
	for (auto &kv : py_config_dict) {
		auto key = py::str(kv.first);
		auto val = py::str(kv.second);
		config_dict[key] = val;
	}
	return config_dict;
}

void CreateNewInstance(DuckDBPyConnection &res, const string &database, DBConfig &config) {
	// We don't cache unnamed memory instances (i.e., :memory:)
	bool cache_instance = database != ":memory:" && !database.empty();
	res.database = instance_cache.CreateInstance(database, config, cache_instance);
	res.connection = make_uniq<Connection>(*res.database);
	auto &context = *res.connection->context;
	PandasScanFunction scan_fun;
	CreateTableFunctionInfo scan_info(scan_fun);
	MapFunction map_fun;
	CreateTableFunctionInfo map_info(map_fun);
	auto &catalog = Catalog::GetSystemCatalog(context);
	context.transaction.BeginTransaction();
	catalog.CreateTableFunction(context, &scan_info);
	catalog.CreateTableFunction(context, &map_info);
	context.transaction.Commit();
	auto &db_config = res.database->instance->config;
	db_config.AddExtensionOption("pandas_analyze_sample",
	                             "The maximum number of rows to sample when analyzing a pandas object column.",
	                             LogicalType::UBIGINT, Value::UBIGINT(1000));
	if (db_config.options.enable_external_access) {
		db_config.replacement_scans.emplace_back(ScanReplacement);
	}
}

static bool HasJupyterProgressBarDependencies() {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	if (!import_cache.ipywidgets().IsLoaded()) {
		// ipywidgets not installed, needed to support the progress bar
		return false;
	}
	return true;
}

static void SetDefaultConfigArguments(ClientContext &context) {
	if (!DuckDBPyConnection::IsInteractive()) {
		// Don't need to set any special default arguments
		return;
	}

	auto &config = ClientConfig::GetConfig(context);
	config.enable_progress_bar = true;

	if (!DuckDBPyConnection::IsJupyter()) {
		return;
	}
	if (!HasJupyterProgressBarDependencies()) {
		// Disable progress bar altogether
		config.system_progress_bar_disable_reason =
		    "required package 'ipywidgets' is missing, which is needed to render progress bars in Jupyter";
		config.enable_progress_bar = false;
		return;
	}

	// Set the function used to create the display for the progress bar
	context.config.display_create_func = JupyterProgressBarDisplay::Create;
}

static shared_ptr<DuckDBPyConnection> FetchOrCreateInstance(const string &database, DBConfig &config) {
	auto res = make_shared<DuckDBPyConnection>();
	res->database = instance_cache.GetInstance(database, config);
	if (!res->database) {
		//! No cached database, we must create a new instance
		CreateNewInstance(*res, database, config);
		return res;
	}
	res->connection = make_uniq<Connection>(*res->database);
	return res;
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::Connect(const string &database, bool read_only,
                                                           const py::dict &config_options) {
	auto config_dict = TransformPyConfigDict(config_options);
	DBConfig config(config_dict, read_only);

	auto res = FetchOrCreateInstance(database, config);
	auto &client_context = *res->connection->context;
	SetDefaultConfigArguments(client_context);
	return res;
}

vector<Value> DuckDBPyConnection::TransformPythonParamList(const py::handle &params) {
	vector<Value> args;
	args.reserve(py::len(params));

	for (auto param : params) {
		args.emplace_back(TransformPythonValue(param, LogicalType::UNKNOWN, false));
	}
	return args;
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::DefaultConnection() {
	if (!default_connection) {
		py::dict config_dict;
		default_connection = DuckDBPyConnection::Connect(":memory:", false, config_dict);
	}
	return default_connection;
}

PythonImportCache *DuckDBPyConnection::ImportCache() {
	if (!import_cache) {
		import_cache = make_shared<PythonImportCache>();
	}
	return import_cache.get();
}

ModifiedMemoryFileSystem &DuckDBPyConnection::GetObjectFileSystem() {
	if (!internal_object_filesystem) {
		D_ASSERT(!FileSystemIsRegistered("DUCKDB_INTERNAL_OBJECTSTORE"));
		auto &import_cache_py = *ImportCache();
		internal_object_filesystem =
		    make_shared<ModifiedMemoryFileSystem>(import_cache_py.pyduckdb().filesystem.modified_memory_filesystem()());
		auto &abstract_fs = (AbstractFileSystem &)*internal_object_filesystem;
		RegisterFilesystem(abstract_fs);
	}
	return *internal_object_filesystem;
}

bool DuckDBPyConnection::IsInteractive() {
	return DuckDBPyConnection::environment != PythonEnvironmentType::NORMAL;
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::Enter() {
	return shared_from_this();
}

bool DuckDBPyConnection::Exit(DuckDBPyConnection &self, const py::object &exc_type, const py::object &exc,
                              const py::object &traceback) {
	self.Close();
	if (exc_type.ptr() != Py_None) {
		return false;
	}
	return true;
}

void DuckDBPyConnection::Cleanup() {
	default_connection.reset();
	import_cache.reset();
}

bool DuckDBPyConnection::IsPandasDataframe(const py::object &object) {
	if (!ModuleIsLoaded<PandasCacheItem>()) {
		return false;
	}
	auto &import_cache_py = *DuckDBPyConnection::ImportCache();
	return import_cache_py.pandas().DataFrame.IsInstance(object);
}

bool DuckDBPyConnection::IsPolarsDataframe(const py::object &object) {
	if (!ModuleIsLoaded<PolarsCacheItem>()) {
		return false;
	}
	auto &import_cache_py = *DuckDBPyConnection::ImportCache();
	return import_cache_py.polars().DataFrame.IsInstance(object) ||
	       import_cache_py.polars().LazyFrame.IsInstance(object);
}

bool DuckDBPyConnection::IsAcceptedArrowObject(const py::object &object) {
	if (!ModuleIsLoaded<ArrowCacheItem>()) {
		return false;
	}
	auto &import_cache_py = *DuckDBPyConnection::ImportCache();
	return import_cache_py.arrow().lib.Table.IsInstance(object) ||
	       import_cache_py.arrow().lib.RecordBatchReader.IsInstance(object) ||
	       import_cache_py.arrow().dataset.Dataset.IsInstance(object) ||
	       import_cache_py.arrow().dataset.Scanner.IsInstance(object);
}

} // namespace duckdb
