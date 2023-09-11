#include "duckdb_python/pybind11/pybind_wrapper.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parser.hpp"

#include "duckdb_python/python_objects.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/expression/pyexpression.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/pybind11/exceptions.hpp"
#include "duckdb_python/typing.hpp"
#include "duckdb_python/functional.hpp"
#include "duckdb_python/connection_wrapper.hpp"
#include "duckdb_python/pybind11/conversions/pyconnection_default.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb_python/pybind11/conversions/exception_handling_enum.hpp"
#include "duckdb_python/pybind11/conversions/python_udf_type_enum.hpp"

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

static py::list PyTokenize(const string &query) {
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
	return result;
}

static void InitializeConnectionMethods(py::module_ &m) {
	m.def("cursor", &PyConnectionWrapper::Cursor, "Create a duplicate of the current connection",
	      py::arg("connection") = py::none())
	    .def("duplicate", &PyConnectionWrapper::Cursor, "Create a duplicate of the current connection",
	         py::arg("connection") = py::none());
	m.def("create_function", &PyConnectionWrapper::RegisterScalarUDF,
	      "Create a DuckDB function out of the passing in python function so it can be used in queries",
	      py::arg("name"), py::arg("function"), py::arg("return_type") = py::none(), py::arg("parameters") = py::none(),
	      py::kw_only(), py::arg("type") = PythonUDFType::NATIVE, py::arg("null_handling") = 0,
	      py::arg("exception_handling") = 0, py::arg("side_effects") = false, py::arg("connection") = py::none());

	m.def("remove_function", &PyConnectionWrapper::UnregisterUDF, "Remove a previously created function",
	      py::arg("name"), py::arg("connection") = py::none());

	DefineMethod({"sqltype", "dtype", "type"}, m, &PyConnectionWrapper::Type, "Create a type object from 'type_str'",
	             py::arg("type_str"), py::arg("connection") = py::none());
	DefineMethod({"struct_type", "row_type"}, m, &PyConnectionWrapper::StructType,
	             "Create a struct type object from 'fields'", py::arg("fields"), py::arg("connection") = py::none());
	m.def("union_type", &PyConnectionWrapper::UnionType, "Create a union type object from 'members'",
	      py::arg("members").none(false), py::arg("connection") = py::none())
	    .def("string_type", &PyConnectionWrapper::StringType, "Create a string type with an optional collation",
	         py::arg("collation") = string(), py::arg("connection") = py::none())
	    .def("enum_type", &PyConnectionWrapper::EnumType,
	         "Create an enum type of underlying 'type', consisting of the list of 'values'", py::arg("name"),
	         py::arg("type"), py::arg("values"), py::arg("connection") = py::none())
	    .def("decimal_type", &PyConnectionWrapper::DecimalType, "Create a decimal type with 'width' and 'scale'",
	         py::arg("width"), py::arg("scale"), py::arg("connection") = py::none());
	DefineMethod({"array_type", "list_type"}, m, &PyConnectionWrapper::ArrayType,
	             "Create an array type object of 'type'", py::arg("type").none(false),
	             py::arg("connection") = py::none());
	m.def("map_type", &PyConnectionWrapper::MapType, "Create a map type object from 'key_type' and 'value_type'",
	      py::arg("key").none(false), py::arg("value").none(false), py::arg("connection") = py::none())
	    .def("execute", &PyConnectionWrapper::Execute,
	         "Execute the given SQL query, optionally using prepared statements with parameters set", py::arg("query"),
	         py::arg("parameters") = py::none(), py::arg("multiple_parameter_sets") = false,
	         py::arg("connection") = py::none())
	    .def("executemany", &PyConnectionWrapper::ExecuteMany,
	         "Execute the given prepared statement multiple times using the list of parameter sets in parameters",
	         py::arg("query"), py::arg("parameters") = py::none(), py::arg("connection") = py::none())
	    .def("close", &PyConnectionWrapper::Close, "Close the connection", py::arg("connection") = py::none())
	    .def("interrupt", &PyConnectionWrapper::Interrupt, "Interrupt pending operations",
	         py::arg("connection") = py::none())
	    .def("fetchone", &PyConnectionWrapper::FetchOne, "Fetch a single row from a result following execute",
	         py::arg("connection") = py::none())
	    .def("fetchmany", &PyConnectionWrapper::FetchMany, "Fetch the next set of rows from a result following execute",
	         py::arg("size") = 1, py::arg("connection") = py::none())
	    .def("fetchall", &PyConnectionWrapper::FetchAll, "Fetch all rows from a result following execute",
	         py::arg("connection") = py::none())
	    .def("fetchnumpy", &PyConnectionWrapper::FetchNumpy, "Fetch a result as list of NumPy arrays following execute",
	         py::arg("connection") = py::none())
	    .def("fetchdf", &PyConnectionWrapper::FetchDF, "Fetch a result as DataFrame following execute()", py::kw_only(),
	         py::arg("date_as_object") = false, py::arg("connection") = py::none())
	    .def("fetch_df", &PyConnectionWrapper::FetchDF, "Fetch a result as DataFrame following execute()",
	         py::kw_only(), py::arg("date_as_object") = false, py::arg("connection") = py::none())
	    .def("fetch_df_chunk", &PyConnectionWrapper::FetchDFChunk,
	         "Fetch a chunk of the result as DataFrame following execute()", py::arg("vectors_per_chunk") = 1,
	         py::kw_only(), py::arg("date_as_object") = false, py::arg("connection") = py::none())
	    .def("df", &PyConnectionWrapper::FetchDF, "Fetch a result as DataFrame following execute()", py::kw_only(),
	         py::arg("date_as_object") = false, py::arg("connection") = py::none())
	    .def("fetch_arrow_table", &PyConnectionWrapper::FetchArrow, "Fetch a result as Arrow table following execute()",
	         py::arg("rows_per_batch") = 1000000, py::arg("connection") = py::none())
	    .def("torch", &PyConnectionWrapper::FetchPyTorch,
	         "Fetch a result as dict of PyTorch Tensors following execute()", py::arg("connection") = py::none())
	    .def("tf", &PyConnectionWrapper::FetchTF, "Fetch a result as dict of TensorFlow Tensors following execute()",
	         py::arg("connection") = py::none())
	    .def("fetch_record_batch", &PyConnectionWrapper::FetchRecordBatchReader,
	         "Fetch an Arrow RecordBatchReader following execute()", py::arg("rows_per_batch") = 1000000,
	         py::arg("connection") = py::none())
	    .def("arrow", &PyConnectionWrapper::FetchArrow, "Fetch a result as Arrow table following execute()",
	         py::arg("rows_per_batch") = 1000000, py::arg("connection") = py::none())
	    .def("pl", &PyConnectionWrapper::FetchPolars, "Fetch a result as Polars DataFrame following execute()",
	         py::arg("rows_per_batch") = 1000000, py::arg("connection") = py::none())
	    .def("begin", &PyConnectionWrapper::Begin, "Start a new transaction", py::arg("connection") = py::none())
	    .def("commit", &PyConnectionWrapper::Commit, "Commit changes performed within a transaction",
	         py::arg("connection") = py::none())
	    .def("rollback", &PyConnectionWrapper::Rollback, "Roll back changes performed within a transaction",
	         py::arg("connection") = py::none())
	    .def("read_json", &PyConnectionWrapper::ReadJSON, "Create a relation object from the JSON file in 'name'",
	         py::arg("name"), py::arg("connection") = py::none(), py::arg("columns") = py::none(),
	         py::arg("sample_size") = py::none(), py::arg("maximum_depth") = py::none(),
	         py::arg("records") = py::none(), py::arg("format") = py::none());

	m.def("values", &PyConnectionWrapper::Values, "Create a relation object from the passed values", py::arg("values"),
	      py::arg("connection") = py::none());
	m.def("from_substrait", &PyConnectionWrapper::FromSubstrait, "Creates a query object from the substrait plan",
	      py::arg("proto"), py::arg("connection") = py::none());
	m.def("get_substrait", &PyConnectionWrapper::GetSubstrait, "Serialize a query object to protobuf", py::arg("query"),
	      py::arg("connection") = py::none(), py::kw_only(), py::arg("enable_optimizer") = true);
	m.def("get_substrait_json", &PyConnectionWrapper::GetSubstraitJSON, "Serialize a query object to protobuf",
	      py::arg("query"), py::arg("connection") = py::none(), py::kw_only(), py::arg("enable_optimizer") = true);
	m.def("from_substrait_json", &PyConnectionWrapper::FromSubstraitJSON, "Serialize a query object to protobuf",
	      py::arg("json"), py::arg("connection") = py::none());
	m.def("df", &PyConnectionWrapper::FromDF, "Create a relation object from the DataFrame df", py::arg("df"),
	      py::arg("connection") = py::none());
	m.def("from_df", &PyConnectionWrapper::FromDF, "Create a relation object from the DataFrame df", py::arg("df"),
	      py::arg("connection") = py::none());
	m.def("from_arrow", &PyConnectionWrapper::FromArrow, "Create a relation object from an Arrow object",
	      py::arg("arrow_object"), py::arg("connection") = py::none());
	m.def("arrow", &PyConnectionWrapper::FromArrow, "Create a relation object from an Arrow object",
	      py::arg("arrow_object"), py::arg("connection") = py::none());
	m.def("filter", &PyConnectionWrapper::FilterDf, "Filter the DataFrame df by the filter in filter_expr",
	      py::arg("df"), py::arg("filter_expr"), py::arg("connection") = py::none());
	m.def("project", &PyConnectionWrapper::ProjectDf, "Project the DataFrame df by the projection in project_expr",
	      py::arg("df"), py::arg("project_expr"), py::arg("connection") = py::none());
	m.def("alias", &PyConnectionWrapper::AliasDF, "Create a relation from DataFrame df with the passed alias",
	      py::arg("df"), py::arg("alias"), py::arg("connection") = py::none());
	m.def("order", &PyConnectionWrapper::OrderDf, "Reorder the DataFrame df by order_expr", py::arg("df"),
	      py::arg("order_expr"), py::arg("connection") = py::none());
	m.def("aggregate", &PyConnectionWrapper::AggregateDF,
	      "Compute the aggregate aggr_expr by the optional groups group_expr on DataFrame df", py::arg("df"),
	      py::arg("aggr_expr"), py::arg("group_expr") = "", py::arg("connection") = py::none());
	m.def("distinct", &PyConnectionWrapper::DistinctDF, "Compute the distinct rows from DataFrame df ", py::arg("df"),
	      py::arg("connection") = py::none());
	m.def("limit", &PyConnectionWrapper::LimitDF, "Retrieve the first n rows from the DataFrame df", py::arg("df"),
	      py::arg("n"), py::arg("connection") = py::none());

	m.def("query_df", &PyConnectionWrapper::QueryDF,
	      "Run the given SQL query in sql_query on the view named virtual_table_name that contains the content of "
	      "DataFrame df",
	      py::arg("df"), py::arg("virtual_table_name"), py::arg("sql_query"), py::arg("connection") = py::none());

	m.def("write_csv", &PyConnectionWrapper::WriteCsvDF, "Write the DataFrame df to a CSV file in file_name",
	      py::arg("df"), py::arg("file_name"), py::arg("connection") = py::none());

	DefineMethod(
	    {"read_csv", "from_csv_auto"}, m, &PyConnectionWrapper::ReadCSV,
	    "Create a relation object from the CSV file in 'name'", py::arg("name"), py::arg("connection") = py::none(),
	    py::arg("header") = py::none(), py::arg("compression") = py::none(), py::arg("sep") = py::none(),
	    py::arg("delimiter") = py::none(), py::arg("dtype") = py::none(), py::arg("na_values") = py::none(),
	    py::arg("skiprows") = py::none(), py::arg("quotechar") = py::none(), py::arg("escapechar") = py::none(),
	    py::arg("encoding") = py::none(), py::arg("parallel") = py::none(), py::arg("date_format") = py::none(),
	    py::arg("timestamp_format") = py::none(), py::arg("sample_size") = py::none(),
	    py::arg("all_varchar") = py::none(), py::arg("normalize_names") = py::none(), py::arg("filename") = py::none(),
	    py::arg("null_padding") = py::none());

	m.def("append", &PyConnectionWrapper::Append, "Append the passed DataFrame to the named table",
	      py::arg("table_name"), py::arg("df"), py::kw_only(), py::arg("by_name") = false,
	      py::arg("connection") = py::none())
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
	         py::arg("parameters") = py::none(), py::arg("connection") = py::none());

	DefineMethod({"sql", "query", "from_query"}, m, &PyConnectionWrapper::RunQuery,
	             "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, "
	             "otherwise run the query as-is.",
	             py::arg("query"), py::arg("alias") = "", py::arg("connection") = py::none());

	DefineMethod({"from_parquet", "read_parquet"}, m, &PyConnectionWrapper::FromParquet,
	             "Create a relation object from the Parquet files in file_glob", py::arg("file_glob"),
	             py::arg("binary_as_string") = false, py::kw_only(), py::arg("file_row_number") = false,
	             py::arg("filename") = false, py::arg("hive_partitioning") = false, py::arg("union_by_name") = false,
	             py::arg("compression") = py::none(), py::arg("connection") = py::none());

	DefineMethod({"from_parquet", "read_parquet"}, m, &PyConnectionWrapper::FromParquets,
	             "Create a relation object from the Parquet files in file_globs", py::arg("file_globs"),
	             py::arg("binary_as_string") = false, py::kw_only(), py::arg("file_row_number") = false,
	             py::arg("filename") = false, py::arg("hive_partitioning") = false, py::arg("union_by_name") = false,
	             py::arg("compression") = py::none(), py::arg("connection") = py::none());

	m.def("from_substrait", &PyConnectionWrapper::FromSubstrait, "Create a query object from protobuf plan",
	      py::arg("proto"), py::arg("connection") = py::none())
	    .def("get_substrait", &PyConnectionWrapper::GetSubstrait, "Serialize a query to protobuf", py::arg("query"),
	         py::arg("connection") = py::none(), py::kw_only(), py::arg("enable_optimizer") = true)
	    .def("get_substrait_json", &PyConnectionWrapper::GetSubstraitJSON,
	         "Serialize a query to protobuf on the JSON format", py::arg("query"), py::arg("connection") = py::none(),
	         py::kw_only(), py::arg("enable_optimizer") = true)
	    .def("get_table_names", &PyConnectionWrapper::GetTableNames, "Extract the required table names from a query",
	         py::arg("query"), py::arg("connection") = py::none())
	    .def("description", &PyConnectionWrapper::GetDescription, "Get result set attributes, mainly column names",
	         py::arg("connection") = py::none())
	    .def("install_extension", &PyConnectionWrapper::InstallExtension, "Install an extension by name",
	         py::arg("extension"), py::kw_only(), py::arg("force_install") = false, py::arg("connection") = py::none())
	    .def("load_extension", &PyConnectionWrapper::LoadExtension, "Load an installed extension", py::arg("extension"),
	         py::arg("connection") = py::none())
	    .def("register_filesystem", &PyConnectionWrapper::RegisterFilesystem, "Register a fsspec compliant filesystem",
	         py::arg("filesystem"), py::arg("connection") = py::none())
	    .def("unregister_filesystem", &PyConnectionWrapper::UnregisterFilesystem, "Unregister a filesystem",
	         py::arg("name"), py::arg("connection") = py::none())
	    .def("list_filesystems", &PyConnectionWrapper::ListFilesystems,
	         "List registered filesystems, including builtin ones", py::arg("connection") = py::none())
	    .def("filesystem_is_registered", &PyConnectionWrapper::FileSystemIsRegistered,
	         "Check if a filesystem with the provided name is currently registered", py::arg("name"),
	         py::arg("connection") = py::none());
}

PYBIND11_MODULE(DUCKDB_PYTHON_LIB_NAME, m) { // NOLINT
	py::enum_<duckdb::ExplainType>(m, "ExplainType")
	    .value("STANDARD", duckdb::ExplainType::EXPLAIN_STANDARD)
	    .value("ANALYZE", duckdb::ExplainType::EXPLAIN_ANALYZE)
	    .export_values();

	py::enum_<duckdb::PythonExceptionHandling>(m, "PythonExceptionHandling")
	    .value("DEFAULT", duckdb::PythonExceptionHandling::FORWARD_ERROR)
	    .value("RETURN_NULL", duckdb::PythonExceptionHandling::RETURN_NULL)
	    .export_values();

	DuckDBPyTyping::Initialize(m);
	DuckDBPyFunctional::Initialize(m);
	DuckDBPyExpression::Initialize(m);
	DuckDBPyRelation::Initialize(m);
	DuckDBPyConnection::Initialize(m);
	PythonObject::Initialize();

	py::options pybind_opts;

	m.doc() = "DuckDB is an embeddable SQL OLAP Database Management System";
	m.attr("__package__") = "duckdb";
	m.attr("__version__") = DuckDB::LibraryVersion();
	m.attr("__standard_vector_size__") = DuckDB::StandardVectorSize();
	m.attr("__git_revision__") = DuckDB::SourceID();
	m.attr("__interactive__") = DuckDBPyConnection::DetectAndGetEnvironment();
	m.attr("__jupyter__") = DuckDBPyConnection::IsJupyter();
	m.attr("default_connection") = DuckDBPyConnection::DefaultConnection();
	m.attr("apilevel") = "1.0";
	m.attr("threadsafety") = 1;
	m.attr("paramstyle") = "qmark";

	InitializeConnectionMethods(m);

	RegisterExceptions(m);

	m.def("connect", &DuckDBPyConnection::Connect,
	      "Create a DuckDB database instance. Can take a database file name to read/write persistent data and a "
	      "read_only flag if no changes are desired",
	      py::arg("database") = ":memory:", py::arg("read_only") = false, py::arg_v("config", py::dict(), "None"));
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

	// we need this because otherwise we try to remove registered_dfs on shutdown when python is already dead
	auto clean_default_connection = []() {
		DuckDBPyConnection::Cleanup();
	};
	m.add_object("_clean_default_connection", py::capsule(clean_default_connection));
}

} // namespace duckdb
