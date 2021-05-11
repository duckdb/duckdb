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
#include "duckdb/common/printer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "extension/extension_helper.hpp"

#include "datetime.h" // from Python

#include <random>

namespace duckdb {

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::default_connection = nullptr;

void DuckDBPyConnection::Initialize(py::handle &m) {
	py::class_<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>>(m, "DuckDBPyConnection")
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
	         "Fetch a chunk of the result as Data.Frame following execute()")
	    .def("df", &DuckDBPyConnection::FetchDF, "Fetch a result as Data.Frame following execute()")
	    .def("fetch_arrow_table", &DuckDBPyConnection::FetchArrow, "Fetch a result as Arrow table following execute()")
	    .def("arrow", &DuckDBPyConnection::FetchArrow, "Fetch a result as Arrow table following execute()")
	    .def("begin", &DuckDBPyConnection::Begin, "Start a new transaction")
	    .def("commit", &DuckDBPyConnection::Commit, "Commit changes performed within a transaction")
	    .def("rollback", &DuckDBPyConnection::Rollback, "Roll back changes performed within a transaction")
	    .def("append", &DuckDBPyConnection::Append, "Append the passed Data.Frame to the named table",
	         py::arg("table_name"), py::arg("df"))
	    .def("register", &DuckDBPyConnection::RegisterDF,
	         "Register the passed Data.Frame value for querying with a view", py::arg("view_name"), py::arg("df"))
	    .def("unregister", &DuckDBPyConnection::UnregisterDF, "Unregister the view name", py::arg("view_name"))
	    .def("register_arrow", &DuckDBPyConnection::RegisterArrow,
	         "Register the passed Arrow Table for querying with a view", py::arg("view_name"), py::arg("arrow_table"))
	    .def("unregister_arrow", &DuckDBPyConnection::UnregisterArrow, "Unregister the view name", py::arg("view_name"))
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
	    .def("query", &DuckDBPyConnection::FromQuery, "Create a relation object from the given SQL query",
	         py::arg("query"), py::arg("alias") = "query_relation")
	    .def("from_df", &DuckDBPyConnection::FromDF, "Create a relation object from the Data.Frame in df",
	         py::arg("df") = py::none())
	    .def("from_arrow_table", &DuckDBPyConnection::FromArrowTable, "Create a relation object from an Arrow table",
	         py::arg("table"))
	    .def("df", &DuckDBPyConnection::FromDF, "Create a relation object from the Data.Frame in df (alias of from_df)",
	         py::arg("df"))
	    .def("from_csv_auto", &DuckDBPyConnection::FromCsvAuto,
	         "Create a relation object from the CSV file in file_name", py::arg("file_name"))
	    .def("from_parquet", &DuckDBPyConnection::FromParquet,
	         "Create a relation object from the Parquet file in file_name", py::arg("file_name"))
	    .def("__getattr__", &DuckDBPyConnection::GetAttr, "Get result set attributes, mainly column names");

	PyDateTime_IMPORT;
}

DuckDBPyConnection::~DuckDBPyConnection() {
	for (auto &element : registered_dfs) {
		UnregisterDF(element.first);
	}
	for (auto &element : registered_arrow) {
		UnregisterArrow(element.first);
	}
}

DuckDBPyConnection *DuckDBPyConnection::ExecuteMany(const string &query, py::object params) {
	Execute(query, std::move(params), true);
	return this;
}

DuckDBPyConnection *DuckDBPyConnection::Execute(const string &query, py::object params, bool many) {
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

DuckDBPyConnection *DuckDBPyConnection::Append(const string &name, py::object value) {
	RegisterDF("__append_df", std::move(value));
	return Execute("INSERT INTO \"" + name + "\" SELECT * FROM __append_df");
}

DuckDBPyConnection *DuckDBPyConnection::RegisterDF(const string &name, py::object value) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	connection->TableFunction("pandas_scan", {Value::POINTER((uintptr_t)value.ptr())})->CreateView(name, true, true);
	// keep a reference
	registered_dfs[name] = value;
	return this;
}

DuckDBPyConnection *DuckDBPyConnection::RegisterArrow(const string &name, py::object table) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	if (table.is_none() || string(py::str(table.get_type().attr("__name__"))) != "Table") {
		throw std::runtime_error("Only arrow tables supported");
	}
	auto stream_factory = make_unique<PythonTableArrowArrayStreamFactory>(table.ptr());
	registered_arrow[name] = table;

	unique_ptr<ArrowArrayStream> (*stream_factory_produce)(uintptr_t factory) =
	    PythonTableArrowArrayStreamFactory::Produce;
	connection
	    ->TableFunction("arrow_scan", {Value::POINTER((uintptr_t)stream_factory.get()),
	                                   Value::POINTER((uintptr_t)stream_factory_produce)})
	    ->CreateView(name, true, true);
	registered_arrow_factory[name] = move(stream_factory);
	return this;
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromQuery(const string &query, const string &alias) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	return make_unique<DuckDBPyRelation>(connection->RelationFromQuery(query, alias));
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
	registered_dfs[name] = value;
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

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromParquet(const string &filename) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}
	vector<Value> params;
	params.emplace_back(filename);
	return make_unique<DuckDBPyRelation>(connection->TableFunction("parquet_scan", params)->Alias(filename));
}

unique_ptr<DuckDBPyRelation> DuckDBPyConnection::FromArrowTable(py::object table) {
	if (!connection) {
		throw std::runtime_error("connection closed");
	}

	// the following is a careful dance around having to depend on pyarrow
	if (table.is_none() || string(py::str(table.get_type().attr("__name__"))) != "Table") {
		throw std::runtime_error("Only arrow tables supported");
	}
	string name = "arrow_table_" + GenerateRandomName();

	auto stream_factory = make_unique<PythonTableArrowArrayStreamFactory>(table.ptr());
	registered_arrow[name] = table;

	unique_ptr<ArrowArrayStream> (*stream_factory_produce)(uintptr_t factory) =
	    PythonTableArrowArrayStreamFactory::Produce;
	auto rel = make_unique<DuckDBPyRelation>(
	    connection
	        ->TableFunction("arrow_scan", {Value::POINTER((uintptr_t)stream_factory.get()),
	                                       Value::POINTER((uintptr_t)stream_factory_produce)})
	        ->Alias(name));
	registered_arrow_factory[name] = move(stream_factory);
	return rel;
}

DuckDBPyConnection *DuckDBPyConnection::UnregisterDF(const string &name) {
	registered_dfs[name] = py::none();
	if (connection) {
		connection->Query("DROP VIEW \"" + name + "\"");
	}
	return this;
}

DuckDBPyConnection *DuckDBPyConnection::UnregisterArrow(const string &name) {
	if (registered_arrow_factory[name]) {
		registered_arrow_factory[name].reset();
	}
	if (registered_arrow[name]) {
		registered_arrow[name] = py::none();
	}
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

py::object DuckDBPyConnection::GetAttr(const py::str &key) {
	if (key.cast<string>() == "description") {
		if (!result) {
			throw std::runtime_error("no open result set");
		}
		return result->Description();
	}
	return py::none();
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
	auto res = make_shared<DuckDBPyConnection>();
	res->database = database;
	res->connection = make_unique<Connection>(*res->database);
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
	return result->FetchNumpy();
}
py::object DuckDBPyConnection::FetchDF() {
	if (!result) {
		throw std::runtime_error("no open result set");
	}
	return result->FetchDF();
}

py::object DuckDBPyConnection::FetchDFChunk() const {
	if (!result) {
		throw std::runtime_error("no open result set");
	}
	return result->FetchDFChunk();
}

py::object DuckDBPyConnection::FetchArrow() {
	if (!result) {
		throw std::runtime_error("no open result set");
	}
	return result->FetchArrowTable();
}

static unique_ptr<TableFunctionRef> TryPandasReplacement(py::dict &dict, py::str &table_name) {
	if (!dict.contains(table_name)) {
		// not present in the globals
		return nullptr;
	}
	auto entry = dict[table_name];

	// check if there is a local or global variable
	auto table_function = make_unique<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ConstantExpression>(Value::POINTER((uintptr_t)entry.ptr())));
	table_function->function = make_unique<FunctionExpression>("pandas_scan", children);
	return table_function;
}

static unique_ptr<TableFunctionRef> PandasScanReplacement(const string &table_name, void *data) {
	// look in the locals first
	PyObject *p = PyEval_GetLocals();
	auto py_table_name = py::str(table_name);
	if (p) {
		auto local_dict = py::reinterpret_borrow<py::dict>(p);
		auto result = TryPandasReplacement(local_dict, py_table_name);
		if (result) {
			return result;
		}
	}
	// otherwise look in the globals
	auto global_dict = py::globals();
	return TryPandasReplacement(global_dict, py_table_name);
}

shared_ptr<DuckDBPyConnection> DuckDBPyConnection::Connect(const string &database, bool read_only) {
	auto res = make_shared<DuckDBPyConnection>();
	DBConfig config;
	if (read_only) {
		config.access_mode = AccessMode::READ_ONLY;
	}
	config.replacement_scans.emplace_back(PandasScanReplacement);

	res->database = make_unique<DuckDB>(database, &config);
	ExtensionHelper::LoadAllExtensions(*res->database);
	res->connection = make_unique<Connection>(*res->database);

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

DuckDBPyConnection *DuckDBPyConnection::DefaultConnection() {
	if (!default_connection) {
		default_connection = DuckDBPyConnection::Connect(":memory:", false);
	}
	return default_connection.get();
}

void DuckDBPyConnection::Cleanup() {
	default_connection.reset();
}

} // namespace duckdb
