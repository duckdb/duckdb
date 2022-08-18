#include "duckdb_python/pybind_wrapper.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parser.hpp"

#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/exceptions.hpp"

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

PYBIND11_MODULE(DUCKDB_PYTHON_LIB_NAME, m) {
	DuckDBPyRelation::Initialize(m);
	DuckDBPyResult::Initialize(m);
	DuckDBPyConnection::Initialize(m);

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
	      py::arg("database") = ":memory:", py::arg("read_only") = false, py::arg("config") = py::dict());
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
	      py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("from_query", &DuckDBPyRelation::FromQuery, "Create a relation object from the given SQL query",
	      py::arg("query"), py::arg("alias") = "query_relation",
	      py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("query", &DuckDBPyRelation::RunQuery,
	      "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
	      "run the query as-is.",
	      py::arg("query"), py::arg("alias") = "query_relation",
	      py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("from_csv_auto", &DuckDBPyRelation::FromCsvAuto, "Creates a relation object from the CSV file in file_name",
	      py::arg("file_name"), py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("from_substrait", &DuckDBPyRelation::FromSubstrait, "Creates a query object from the substrait plan",
	      py::arg("proto"), py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("get_substrait", &DuckDBPyRelation::GetSubstrait, "Serialize a query object to protobuf", py::arg("query"),
	      py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("get_substrait_json", &DuckDBPyRelation::GetSubstraitJSON, "Serialize a query object to protobuf",
	      py::arg("query"), py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("from_parquet", &DuckDBPyRelation::FromParquet,
	      "Creates a relation object from the Parquet file in file_name", py::arg("file_name"),
	      py::arg("binary_as_string"), py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("from_parquet", &DuckDBPyRelation::FromParquetDefault,
	      "Creates a relation object from the Parquet file in file_name", py::arg("file_name"),
	      py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("df", &DuckDBPyRelation::FromDf, "Create a relation object from the Data.Frame df", py::arg("df"),
	      py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("from_df", &DuckDBPyRelation::FromDf, "Create a relation object from the Data.Frame df", py::arg("df"),
	      py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("from_arrow", &DuckDBPyRelation::FromArrow, "Create a relation object from an Arrow object",
	      py::arg("arrow_object"), py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("arrow", &DuckDBPyRelation::FromArrow, "Create a relation object from an Arrow object",
	      py::arg("arrow_object"), py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("filter", &DuckDBPyRelation::FilterDf, "Filter the Data.Frame df by the filter in filter_expr", py::arg("df"),
	      py::arg("filter_expr"), py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("project", &DuckDBPyRelation::ProjectDf, "Project the Data.Frame df by the projection in project_expr",
	      py::arg("df"), py::arg("project_expr"), py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("alias", &DuckDBPyRelation::AliasDF, "Create a relation from Data.Frame df with the passed alias",
	      py::arg("df"), py::arg("alias"), py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("order", &DuckDBPyRelation::OrderDf, "Reorder the Data.Frame df by order_expr", py::arg("df"),
	      py::arg("order_expr"), py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("aggregate", &DuckDBPyRelation::AggregateDF,
	      "Compute the aggregate aggr_expr by the optional groups group_expr on Data.frame df", py::arg("df"),
	      py::arg("aggr_expr"), py::arg("group_expr") = "",
	      py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("distinct", &DuckDBPyRelation::DistinctDF, "Compute the distinct rows from Data.Frame df ", py::arg("df"),
	      py::arg("connection") = DuckDBPyConnection::DefaultConnection());
	m.def("limit", &DuckDBPyRelation::LimitDF, "Retrieve the first n rows from the Data.Frame df", py::arg("df"),
	      py::arg("n"), py::arg("connection") = DuckDBPyConnection::DefaultConnection());

	m.def("query_df", &DuckDBPyRelation::QueryDF,
	      "Run the given SQL query in sql_query on the view named virtual_table_name that contains the content of "
	      "Data.Frame df",
	      py::arg("df"), py::arg("virtual_table_name"), py::arg("sql_query"),
	      py::arg("connection") = DuckDBPyConnection::DefaultConnection());

	m.def("write_csv", &DuckDBPyRelation::WriteCsvDF, "Write the Data.Frame df to a CSV file in file_name",
	      py::arg("df"), py::arg("file_name"), py::arg("connection") = DuckDBPyConnection::DefaultConnection());

	// we need this because otherwise we try to remove registered_dfs on shutdown when python is already dead
	auto clean_default_connection = []() {
		DuckDBPyConnection::Cleanup();
	};
	m.add_object("_clean_default_connection", py::capsule(clean_default_connection));

	PyDateTime_IMPORT;
}

} // namespace duckdb
