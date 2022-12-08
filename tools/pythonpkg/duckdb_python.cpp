#include "duckdb_python/pybind_wrapper.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parser.hpp"

#include "duckdb_python/python_objects.hpp"
#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/exceptions.hpp"
#include "duckdb_python/connection_wrapper.hpp"

#include "datetime.h" // from Python

#include "duckdb.hpp"

#ifndef DUCKDB_PYTHON_LIB_NAME
#define DUCKDB_PYTHON_LIB_NAME duckdb
#endif

namespace py = pybind11;

namespace duckdb {

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

static void InitializeConnectionMethods(py::module_ &m) {
	m.def("cursor", &PyConnectionWrapper::Cursor, "Create a duplicate of the current connection",
	      py::arg("connection") = py::none())
	    .def("duplicate", &PyConnectionWrapper::Cursor, "Create a duplicate of the current connection",
	         py::arg("connection") = py::none())
	    .def("execute", &PyConnectionWrapper::Execute,
	         "Execute the given SQL query, optionally using prepared statements with parameters set", py::arg("query"),
	         py::arg("parameters") = py::none(), py::arg("multiple_parameter_sets") = false,
	         py::arg("connection") = py::none())
	    .def("executemany", &PyConnectionWrapper::ExecuteMany,
	         "Execute the given prepared statement multiple times using the list of parameter sets in parameters",
	         py::arg("query"), py::arg("parameters") = py::none(), py::arg("connection") = py::none())
	    .def("close", &PyConnectionWrapper::Close, "Close the connection", py::arg("connection") = py::none())
	    .def("fetchone", &PyConnectionWrapper::FetchOne, "Fetch a single row from a result following execute",
	         py::arg("connection") = py::none())
	    .def("fetchmany", &PyConnectionWrapper::FetchMany, "Fetch the next set of rows from a result following execute",
	         py::arg("size") = 1, py::arg("connection") = py::none())
	    .def("fetchall", &PyConnectionWrapper::FetchAll, "Fetch all rows from a result following execute",
	         py::arg("connection") = py::none())
	    .def("fetchnumpy", &PyConnectionWrapper::FetchNumpy, "Fetch a result as list of NumPy arrays following execute",
	         py::arg("connection") = py::none())
	    .def("fetchdf", &PyConnectionWrapper::FetchDF, "Fetch a result as Data.Frame following execute()",
	         py::kw_only(), py::arg("date_as_object") = false, py::arg("connection") = py::none())
	    .def("fetch_df", &PyConnectionWrapper::FetchDF, "Fetch a result as Data.Frame following execute()",
	         py::kw_only(), py::arg("date_as_object") = false, py::arg("connection") = py::none())
	    .def("fetch_df_chunk", &PyConnectionWrapper::FetchDFChunk,
	         "Fetch a chunk of the result as Data.Frame following execute()", py::arg("vectors_per_chunk") = 1,
	         py::kw_only(), py::arg("date_as_object") = false, py::arg("connection") = py::none())
	    .def("df", &PyConnectionWrapper::FetchDF, "Fetch a result as Data.Frame following execute()", py::kw_only(),
	         py::arg("date_as_object") = false, py::arg("connection") = py::none())
	    .def("fetch_arrow_table", &PyConnectionWrapper::FetchArrow, "Fetch a result as Arrow table following execute()",
	         py::arg("chunk_size") = 1000000, py::arg("connection") = py::none())
	    .def("fetch_record_batch", &PyConnectionWrapper::FetchRecordBatchReader,
	         "Fetch an Arrow RecordBatchReader following execute()", py::arg("chunk_size") = 1000000,
	         py::arg("connection") = py::none())
	    .def("arrow", &PyConnectionWrapper::FetchArrow, "Fetch a result as Arrow table following execute()",
	         py::arg("chunk_size") = 1000000, py::arg("connection") = py::none())
	    .def("begin", &PyConnectionWrapper::Begin, "Start a new transaction", py::arg("connection") = py::none())
	    .def("commit", &PyConnectionWrapper::Commit, "Commit changes performed within a transaction",
	         py::arg("connection") = py::none())
	    .def("rollback", &PyConnectionWrapper::Rollback, "Roll back changes performed within a transaction",
	         py::arg("connection") = py::none())
	    .def("append", &PyConnectionWrapper::Append, "Append the passed Data.Frame to the named table",
	         py::arg("table_name"), py::arg("df"), py::arg("connection") = py::none())
	    .def("register", &PyConnectionWrapper::RegisterPythonObject,
	         "Register the passed Python Object value for querying with a view", py::arg("view_name"),
	         py::arg("python_object"), py::arg("connection") = py::none())
	    .def("unregister", &PyConnectionWrapper::UnregisterPythonObject, "Unregister the view name",
	         py::arg("view_name"), py::arg("connection") = py::none())
	    .def("table", &PyConnectionWrapper::Table, "Create a relation object for the name'd table",
	         py::arg("table_name"), py::arg("connection") = py::none())
	    .def("view", &PyConnectionWrapper::View, "Create a relation object for the name'd view", py::arg("view_name"),
	         py::arg("connection") = py::none())
	    .def("values", &PyConnectionWrapper::Values, "Create a relation object from the passed values",
	         py::arg("values"), py::arg("connection") = py::none())
	    .def("table_function", &PyConnectionWrapper::TableFunction,
	         "Create a relation object from the name'd table function with given parameters", py::arg("name"),
	         py::arg("parameters") = py::none(), py::arg("connection") = py::none())
	    .def("from_query", &PyConnectionWrapper::FromQuery, "Create a relation object from the given SQL query",
	         py::arg("query"), py::arg("alias") = "query_relation", py::arg("connection") = py::none())
	    .def("query", &PyConnectionWrapper::RunQuery,
	         "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, "
	         "otherwise run the query as-is.",
	         py::arg("query"), py::arg("alias") = "query_relation", py::arg("connection") = py::none())
	    .def("from_df", &PyConnectionWrapper::FromDF, "Create a relation object from the Data.Frame in df",
	         py::arg("df") = py::none(), py::arg("connection") = py::none())
	    .def("from_arrow", &PyConnectionWrapper::FromArrow, "Create a relation object from an Arrow object",
	         py::arg("arrow_object"), py::arg("connection") = py::none())
	    .def("from_csv_auto", &PyConnectionWrapper::FromCsvAuto,
	         "Create a relation object from the CSV file in file_name", py::arg("file_name"),
	         py::arg("connection") = py::none())
	    .def("from_parquet", &PyConnectionWrapper::FromParquet,
	         "Create a relation object from the Parquet files in file_glob", py::arg("file_glob"),
	         py::arg("binary_as_string") = false, py::kw_only(), py::arg("file_row_number") = false,
	         py::arg("filename") = false, py::arg("hive_partitioning") = false, py::arg("connection") = py::none())
	    .def("from_parquet", &PyConnectionWrapper::FromParquets,
	         "Create a relation object from the Parquet files in file_globs", py::arg("file_globs"),
	         py::arg("binary_as_string") = false, py::kw_only(), py::arg("file_row_number") = false,
	         py::arg("filename") = false, py::arg("hive_partitioning") = false, py::arg("connection") = py::none())
	    .def("from_substrait", &PyConnectionWrapper::FromSubstrait, "Create a query object from protobuf plan",
	         py::arg("proto"), py::arg("connection") = py::none())
	    .def("get_substrait", &PyConnectionWrapper::GetSubstrait, "Serialize a query to protobuf", py::arg("query"),
	         py::arg("connection") = py::none())
	    .def("get_substrait_json", &PyConnectionWrapper::GetSubstraitJSON,
	         "Serialize a query to protobuf on the JSON format", py::arg("query"), py::arg("connection") = py::none())
	    .def("get_table_names", &PyConnectionWrapper::GetTableNames, "Extract the required table names from a query",
	         py::arg("query"), py::arg("connection") = py::none())
	    .def("description", &PyConnectionWrapper::GetDescription, "Get result set attributes, mainly column names",
	         py::arg("connection") = py::none())
	    .def("install_extension", &PyConnectionWrapper::InstallExtension, "Install an extension by name",
	         py::arg("extension"), py::kw_only(), py::arg("force_install") = false, py::arg("connection") = py::none())
	    .def("load_extension", &PyConnectionWrapper::LoadExtension, "Load an installed extension", py::arg("extension"),
	         py::arg("connection") = py::none());
}

PYBIND11_MODULE(DUCKDB_PYTHON_LIB_NAME, m) {
	DuckDBPyRelation::Initialize(m);
	DuckDBPyConnection::Initialize(m);
	DuckDBPyResult::Initialize(m);
	PythonObject::Initialize();

	InitializeConnectionMethods(m);

	py::options pybind_opts;

	m.doc() = "DuckDB is an embeddable SQL OLAP Database Management System";
	m.attr("__package__") = "duckdb";
	m.attr("__version__") = DuckDB::LibraryVersion();
	m.attr("__git_revision__") = DuckDB::SourceID();
	m.attr("default_connection") = DuckDBPyConnection::DefaultConnection();
	m.attr("apilevel") = "1.0";
	m.attr("threadsafety") = 1;
	m.attr("paramstyle") = "qmark";

	RegisterExceptions(m);

	m.def("connect", &DuckDBPyConnection::Connect,
	      "Create a DuckDB database instance. Can take a database file name to read/write persistent data and a "
	      "read_only flag if no changes are desired",
	      py::arg("database") = ":memory:", py::arg("read_only") = false, py::arg("config") = py::none());
	m.def("tokenize", PyTokenize,
	      "Tokenizes a SQL string, returning a list of (position, type) tuples that can be "
	      "used for e.g. syntax highlighting",
	      py::arg("query"));
	py::enum_<PySQLTokenType>(m, "token_type", py::module_local())
	    .value("identifier", PySQLTokenType::PY_SQL_TOKEN_IDENTIFIER)
	    .value("numeric_const", PySQLTokenType::PY_SQL_TOKEN_NUMERIC_CONSTANT)
	    .value("string_const", PySQLTokenType::PY_SQL_TOKEN_STRING_CONSTANT)
	    .value("operator", PySQLTokenType::PY_SQL_TOKEN_OPERATOR)
	    .value("keyword", PySQLTokenType::PY_SQL_TOKEN_KEYWORD)
	    .value("comment", PySQLTokenType::PY_SQL_TOKEN_COMMENT)
	    .export_values();

	m.def("values", &DuckDBPyRelation::Values, "Create a relation object from the passed values", py::arg("values"),
	      py::arg("connection") = py::none());
	m.def("from_query", &DuckDBPyRelation::FromQuery, "Create a relation object from the given SQL query",
	      py::arg("query"), py::arg("alias") = "query_relation", py::arg("connection") = py::none());
	m.def("query", &DuckDBPyRelation::RunQuery,
	      "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
	      "run the query as-is.",
	      py::arg("query"), py::arg("alias") = "query_relation", py::arg("connection") = py::none());
	m.def("from_csv_auto", &DuckDBPyRelation::FromCsvAuto, "Creates a relation object from the CSV file in file_name",
	      py::arg("file_name"), py::arg("connection") = py::none());
	m.def("from_substrait", &DuckDBPyRelation::FromSubstrait, "Creates a query object from the substrait plan",
	      py::arg("proto"), py::arg("connection") = py::none());
	m.def("get_substrait", &DuckDBPyRelation::GetSubstrait, "Serialize a query object to protobuf", py::arg("query"),
	      py::arg("connection") = py::none());
	m.def("get_substrait_json", &DuckDBPyRelation::GetSubstraitJSON, "Serialize a query object to protobuf",
	      py::arg("query"), py::arg("connection") = py::none());
	m.def("from_parquet", &DuckDBPyRelation::FromParquet,
	      "Creates a relation object from the Parquet files in file_glob", py::arg("file_glob"),
	      py::arg("binary_as_string") = false, py::kw_only(), py::arg("file_row_number") = false,
	      py::arg("filename") = false, py::arg("hive_partitioning") = false, py::arg("connection") = py::none());
	m.def("from_parquet", &DuckDBPyRelation::FromParquets,
	      "Creates a relation object from the Parquet files in file_globs", py::arg("file_globs"),
	      py::arg("binary_as_string") = false, py::kw_only(), py::arg("file_row_number") = false,
	      py::arg("filename") = false, py::arg("hive_partitioning") = false, py::arg("connection") = py::none());
	m.def("df", &DuckDBPyRelation::FromDf, "Create a relation object from the Data.Frame df", py::arg("df"),
	      py::arg("connection") = py::none());
	m.def("from_df", &DuckDBPyRelation::FromDf, "Create a relation object from the Data.Frame df", py::arg("df"),
	      py::arg("connection") = py::none());
	m.def("from_arrow", &DuckDBPyRelation::FromArrow, "Create a relation object from an Arrow object",
	      py::arg("arrow_object"), py::arg("connection") = py::none());
	m.def("arrow", &DuckDBPyRelation::FromArrow, "Create a relation object from an Arrow object",
	      py::arg("arrow_object"), py::arg("connection") = py::none());
	m.def("filter", &DuckDBPyRelation::FilterDf, "Filter the Data.Frame df by the filter in filter_expr", py::arg("df"),
	      py::arg("filter_expr"), py::arg("connection") = py::none());
	m.def("project", &DuckDBPyRelation::ProjectDf, "Project the Data.Frame df by the projection in project_expr",
	      py::arg("df"), py::arg("project_expr"), py::arg("connection") = py::none());
	m.def("alias", &DuckDBPyRelation::AliasDF, "Create a relation from Data.Frame df with the passed alias",
	      py::arg("df"), py::arg("alias"), py::arg("connection") = py::none());
	m.def("order", &DuckDBPyRelation::OrderDf, "Reorder the Data.Frame df by order_expr", py::arg("df"),
	      py::arg("order_expr"), py::arg("connection") = py::none());
	m.def("aggregate", &DuckDBPyRelation::AggregateDF,
	      "Compute the aggregate aggr_expr by the optional groups group_expr on Data.frame df", py::arg("df"),
	      py::arg("aggr_expr"), py::arg("group_expr") = "", py::arg("connection") = py::none());
	m.def("distinct", &DuckDBPyRelation::DistinctDF, "Compute the distinct rows from Data.Frame df ", py::arg("df"),
	      py::arg("connection") = py::none());
	m.def("limit", &DuckDBPyRelation::LimitDF, "Retrieve the first n rows from the Data.Frame df", py::arg("df"),
	      py::arg("n"), py::arg("connection") = py::none());

	m.def("query_df", &DuckDBPyRelation::QueryDF,
	      "Run the given SQL query in sql_query on the view named virtual_table_name that contains the content of "
	      "Data.Frame df",
	      py::arg("df"), py::arg("virtual_table_name"), py::arg("sql_query"), py::arg("connection") = py::none());

	m.def("write_csv", &DuckDBPyRelation::WriteCsvDF, "Write the Data.Frame df to a CSV file in file_name",
	      py::arg("df"), py::arg("file_name"), py::arg("connection") = py::none());

	// we need this because otherwise we try to remove registered_dfs on shutdown when python is already dead
	auto clean_default_connection = []() {
		DuckDBPyConnection::Cleanup();
	};
	m.add_object("_clean_default_connection", py::capsule(clean_default_connection));

	PyDateTime_IMPORT;
}

} // namespace duckdb
