#include "duckdb_python/pybind11/pybind_wrapper.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parser.hpp"

#include "duckdb_python/python_objects.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pystatement.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/expression/pyexpression.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/pybind11/exceptions.hpp"
#include "duckdb_python/typing.hpp"
#include "duckdb_python/functional.hpp"
#include "duckdb_python/pybind11/conversions/pyconnection_default.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb_python/pybind11/conversions/exception_handling_enum.hpp"
#include "duckdb_python/pybind11/conversions/python_udf_type_enum.hpp"
#include "duckdb/common/enums/statement_type.hpp"

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
	m.def("project",
	      [](const PandasDataFrame &df, const py::object &expr,
	         shared_ptr<DuckDBPyConnection> conn) -> unique_ptr<DuckDBPyRelation> {
		      // FIXME: if we want to support passing in DuckDBPyExpressions here
		      // we could also accept 'expr' as a List[DuckDBPyExpression], without changing the signature
		      if (!py::isinstance<py::str>(expr)) {
			      throw InvalidInputException("Please provide 'expr' as a string");
		      }
		      return conn->FromDF(df)->Project(expr);
	      });

	// unique_ptr<DuckDBPyRelation> PyConnectionWrapper::DistinctDF(const PandasDataFrame &df,
	//                                                             shared_ptr<DuckDBPyConnection> conn) {
	//       return conn->FromDF(df)->Distinct();
	//}

	// void PyConnectionWrapper::WriteCsvDF(const PandasDataFrame &df, const string &file,
	//                                     shared_ptr<DuckDBPyConnection> conn) {
	//       return conn->FromDF(df)->ToCSV(file);
	//}

	// unique_ptr<DuckDBPyRelation> PyConnectionWrapper::QueryDF(const PandasDataFrame &df, const string &view_name,
	//                                                          const string &sql_query,
	//                                                          shared_ptr<DuckDBPyConnection> conn) {
	//       return conn->FromDF(df)->Query(view_name, sql_query);
	//}

	// unique_ptr<DuckDBPyRelation> PyConnectionWrapper::AggregateDF(const PandasDataFrame &df, const string &expr,
	//                                                              const string &groups,
	//                                                              shared_ptr<DuckDBPyConnection> conn) {
	//       return conn->FromDF(df)->Aggregate(expr, groups);
	//}

	// unique_ptr<DuckDBPyRelation> PyConnectionWrapper::AliasDF(const PandasDataFrame &df, const string &expr,
	//                                                          shared_ptr<DuckDBPyConnection> conn) {
	//       return conn->FromDF(df)->SetAlias(expr);
	//}

	// unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FilterDf(const PandasDataFrame &df, const string &expr,
	//                                                           shared_ptr<DuckDBPyConnection> conn) {
	//       return conn->FromDF(df)->FilterFromExpression(expr);
	//}

	// unique_ptr<DuckDBPyRelation> PyConnectionWrapper::LimitDF(const PandasDataFrame &df, int64_t n,
	//                                                          shared_ptr<DuckDBPyConnection> conn) {
	//       return conn->FromDF(df)->Limit(n);
	//}

	// unique_ptr<DuckDBPyRelation> PyConnectionWrapper::OrderDf(const PandasDataFrame &df, const string &expr,
	//                                                          shared_ptr<DuckDBPyConnection> conn) {
	//       return conn->FromDF(df)->Order(expr);
	//}
}

static void RegisterStatementType(py::handle &m) {
	auto statement_type = py::enum_<duckdb::StatementType>(m, "StatementType");
	static const duckdb::StatementType TYPES[] = {
	    duckdb::StatementType::INVALID_STATEMENT,      duckdb::StatementType::SELECT_STATEMENT,
	    duckdb::StatementType::INSERT_STATEMENT,       duckdb::StatementType::UPDATE_STATEMENT,
	    duckdb::StatementType::CREATE_STATEMENT,       duckdb::StatementType::DELETE_STATEMENT,
	    duckdb::StatementType::PREPARE_STATEMENT,      duckdb::StatementType::EXECUTE_STATEMENT,
	    duckdb::StatementType::ALTER_STATEMENT,        duckdb::StatementType::TRANSACTION_STATEMENT,
	    duckdb::StatementType::COPY_STATEMENT,         duckdb::StatementType::ANALYZE_STATEMENT,
	    duckdb::StatementType::VARIABLE_SET_STATEMENT, duckdb::StatementType::CREATE_FUNC_STATEMENT,
	    duckdb::StatementType::EXPLAIN_STATEMENT,      duckdb::StatementType::DROP_STATEMENT,
	    duckdb::StatementType::EXPORT_STATEMENT,       duckdb::StatementType::PRAGMA_STATEMENT,
	    duckdb::StatementType::VACUUM_STATEMENT,       duckdb::StatementType::CALL_STATEMENT,
	    duckdb::StatementType::SET_STATEMENT,          duckdb::StatementType::LOAD_STATEMENT,
	    duckdb::StatementType::RELATION_STATEMENT,     duckdb::StatementType::EXTENSION_STATEMENT,
	    duckdb::StatementType::LOGICAL_PLAN_STATEMENT, duckdb::StatementType::ATTACH_STATEMENT,
	    duckdb::StatementType::DETACH_STATEMENT,       duckdb::StatementType::MULTI_STATEMENT,
	    duckdb::StatementType::COPY_DATABASE_STATEMENT};
	static const idx_t AMOUNT = sizeof(TYPES) / sizeof(duckdb::StatementType);
	for (idx_t i = 0; i < AMOUNT; i++) {
		auto &type = TYPES[i];
		statement_type.value(StatementTypeToString(type).c_str(), type);
	}
	statement_type.export_values();
}

static void RegisterExpectedResultType(py::handle &m) {
	auto expected_return_type = py::enum_<duckdb::StatementReturnType>(m, "ExpectedResultType");
	static const duckdb::StatementReturnType TYPES[] = {duckdb::StatementReturnType::QUERY_RESULT,
	                                                    duckdb::StatementReturnType::CHANGED_ROWS,
	                                                    duckdb::StatementReturnType::NOTHING};
	static const idx_t AMOUNT = sizeof(TYPES) / sizeof(duckdb::StatementReturnType);
	for (idx_t i = 0; i < AMOUNT; i++) {
		auto &type = TYPES[i];
		expected_return_type.value(StatementReturnTypeToString(type).c_str(), type);
	}
	expected_return_type.export_values();
}

PYBIND11_MODULE(DUCKDB_PYTHON_LIB_NAME, m) { // NOLINT
	py::enum_<duckdb::ExplainType>(m, "ExplainType")
	    .value("STANDARD", duckdb::ExplainType::EXPLAIN_STANDARD)
	    .value("ANALYZE", duckdb::ExplainType::EXPLAIN_ANALYZE)
	    .export_values();

	RegisterStatementType(m);

	RegisterExpectedResultType(m);

	py::enum_<duckdb::PythonExceptionHandling>(m, "PythonExceptionHandling")
	    .value("DEFAULT", duckdb::PythonExceptionHandling::FORWARD_ERROR)
	    .value("RETURN_NULL", duckdb::PythonExceptionHandling::RETURN_NULL)
	    .export_values();

	py::enum_<duckdb::RenderMode>(m, "RenderMode")
	    .value("ROWS", duckdb::RenderMode::ROWS)
	    .value("COLUMNS", duckdb::RenderMode::COLUMNS)
	    .export_values();

	DuckDBPyTyping::Initialize(m);
	DuckDBPyFunctional::Initialize(m);
	DuckDBPyExpression::Initialize(m);
	DuckDBPyStatement::Initialize(m);
	DuckDBPyRelation::Initialize(m);
	DuckDBPyConnection::Initialize(m);
	PythonObject::Initialize();

	py::options pybind_opts;

	m.doc() = "DuckDB is an embeddable SQL OLAP Database Management System";
	m.attr("__package__") = "duckdb";
	m.attr("__version__") = std::string(DuckDB::LibraryVersion()).substr(1);
	m.attr("__standard_vector_size__") = DuckDB::StandardVectorSize();
	m.attr("__git_revision__") = DuckDB::SourceID();
	m.attr("__interactive__") = DuckDBPyConnection::DetectAndGetEnvironment();
	m.attr("__jupyter__") = DuckDBPyConnection::IsJupyter();
	m.attr("default_connection") = DuckDBPyConnection::DefaultConnection();
	m.attr("apilevel") = "2.0";
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
