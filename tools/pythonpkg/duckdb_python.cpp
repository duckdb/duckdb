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

enum PySQLTokenType : uint8_t {
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

	// START_OF_CONNECTION_METHODS
	m.def(
	    "cursor",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Cursor();
	    },
	    "Create a duplicate of the current connection", py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "register_filesystem",
	    [](AbstractFileSystem filesystem, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->RegisterFilesystem(filesystem);
	    },
	    "Register a fsspec compliant filesystem", py::arg("filesystem"), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "unregister_filesystem",
	    [](const py::str &name, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->UnregisterFilesystem(name);
	    },
	    "Unregister a filesystem", py::arg("name"), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "list_filesystems",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ListFilesystems();
	    },
	    "List registered filesystems, including builtin ones", py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "filesystem_is_registered",
	    [](const string &name, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FileSystemIsRegistered(name);
	    },
	    "Check if a filesystem with the provided name is currently registered", py::arg("name"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "create_function",
	    [](const string &name, const py::function &udf, const py::object &arguments,
	       const shared_ptr<DuckDBPyType> &return_type, PythonUDFType type, FunctionNullHandling null_handling,
	       PythonExceptionHandling exception_handling, bool side_effects = false,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->RegisterScalarUDF(name, udf, arguments, return_type, type, null_handling, exception_handling,
		                                   side_effects);
	    },
	    "Create a DuckDB function out of the passing in Python function so it can be used in queries", py::arg("name"),
	    py::arg("function"), py::arg("parameters") = py::none(), py::arg("return_type") = py::none(), py::kw_only(),
	    py::arg("type") = PythonUDFType::NATIVE, py::arg("null_handling") = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	    py::arg("exception_handling") = PythonExceptionHandling::FORWARD_ERROR, py::arg("side_effects") = false,
	    py::arg("conn") = py::none());
	m.def(
	    "remove_function",
	    [](const string &name, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->UnregisterUDF(name);
	    },
	    "Remove a previously created function", py::arg("name"), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "sqltype",
	    [](const string &type_str, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Type(type_str);
	    },
	    "Create a type object by parsing the 'type_str' string", py::arg("type_str"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "dtype",
	    [](const string &type_str, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Type(type_str);
	    },
	    "Create a type object by parsing the 'type_str' string", py::arg("type_str"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "type",
	    [](const string &type_str, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Type(type_str);
	    },
	    "Create a type object by parsing the 'type_str' string", py::arg("type_str"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "array_type",
	    [](const shared_ptr<DuckDBPyType> &type, idx_t size, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ArrayType(type, size);
	    },
	    "Create an array type object of 'type'", py::arg("type").none(false), py::arg("size"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "list_type",
	    [](const shared_ptr<DuckDBPyType> &type, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ListType(type);
	    },
	    "Create a list type object of 'type'", py::arg("type").none(false), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "union_type",
	    [](const py::object &members, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->UnionType(members);
	    },
	    "Create a union type object from 'members'", py::arg("members").none(false), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "string_type",
	    [](const string &collation = string(), shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->StringType(collation);
	    },
	    "Create a string type with an optional collation", py::arg("collation") = "", py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "enum_type",
	    [](const string &name, const shared_ptr<DuckDBPyType> &type, const py::list &values_p,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->EnumType(name, type, values_p);
	    },
	    "Create an enum type of underlying 'type', consisting of the list of 'values'", py::arg("name"),
	    py::arg("type"), py::arg("values"), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "decimal_type",
	    [](int width, int scale, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->DecimalType(width, scale);
	    },
	    "Create a decimal type with 'width' and 'scale'", py::arg("width"), py::arg("scale"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "struct_type",
	    [](const py::object &fields, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->StructType(fields);
	    },
	    "Create a struct type object from 'fields'", py::arg("fields"), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "row_type",
	    [](const py::object &fields, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->StructType(fields);
	    },
	    "Create a struct type object from 'fields'", py::arg("fields"), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "map_type",
	    [](const shared_ptr<DuckDBPyType> &key_type, const shared_ptr<DuckDBPyType> &value_type,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->MapType(key_type, value_type);
	    },
	    "Create a map type object from 'key_type' and 'value_type'", py::arg("key").none(false),
	    py::arg("value").none(false), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "duplicate",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Cursor();
	    },
	    "Create a duplicate of the current connection", py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "execute",
	    [](const py::object &query, py::object params, bool many = false,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Execute(query, params, many);
	    },
	    "Execute the given SQL query, optionally using prepared statements with parameters set", py::arg("query"),
	    py::arg("parameters") = py::none(), py::arg("multiple_parameter_sets") = false, py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "executemany",
	    [](const py::object &query, py::object params, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ExecuteMany(query, params);
	    },
	    "Execute the given prepared statement multiple times using the list of parameter sets in parameters",
	    py::arg("query"), py::arg("parameters") = py::none(), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "close",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->Close();
	    },
	    "Close the connection", py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "interrupt",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->Interrupt();
	    },
	    "Interrupt pending operations", py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "fetchone",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchOne();
	    },
	    "Fetch a single row from a result following execute", py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "fetchmany",
	    [](idx_t size, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchMany(size);
	    },
	    "Fetch the next set of rows from a result following execute", py::arg("size") = 1, py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "fetchall",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchAll();
	    },
	    "Fetch all rows from a result following execute", py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "fetchnumpy",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchNumpy();
	    },
	    "Fetch a result as list of NumPy arrays following execute", py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "fetchdf",
	    [](bool date_as_object, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchDF(date_as_object);
	    },
	    "Fetch a result as DataFrame following execute()", py::kw_only(), py::arg("date_as_object") = false,
	    py::arg("conn") = py::none());
	m.def(
	    "fetch_df",
	    [](bool date_as_object, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchDF(date_as_object);
	    },
	    "Fetch a result as DataFrame following execute()", py::kw_only(), py::arg("date_as_object") = false,
	    py::arg("conn") = py::none());
	m.def(
	    "df",
	    [](bool date_as_object, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchDF(date_as_object);
	    },
	    "Fetch a result as DataFrame following execute()", py::kw_only(), py::arg("date_as_object") = false,
	    py::arg("conn") = py::none());
	m.def(
	    "fetch_df_chunk",
	    [](const idx_t vectors_per_chunk = 1, bool date_as_object = false,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchDFChunk(vectors_per_chunk, date_as_object);
	    },
	    "Fetch a chunk of the result as DataFrame following execute()", py::arg("vectors_per_chunk") = 1, py::kw_only(),
	    py::arg("date_as_object") = false, py::arg("conn") = py::none());
	m.def(
	    "pl",
	    [](idx_t rows_per_batch, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchPolars(rows_per_batch);
	    },
	    "Fetch a result as Polars DataFrame following execute()", py::arg("rows_per_batch") = 1000000, py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "fetch_arrow_table",
	    [](idx_t rows_per_batch, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchArrow(rows_per_batch);
	    },
	    "Fetch a result as Arrow table following execute()", py::arg("rows_per_batch") = 1000000, py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "arrow",
	    [](idx_t rows_per_batch, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchArrow(rows_per_batch);
	    },
	    "Fetch a result as Arrow table following execute()", py::arg("rows_per_batch") = 1000000, py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "fetch_record_batch",
	    [](const idx_t rows_per_batch, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchRecordBatchReader(rows_per_batch);
	    },
	    "Fetch an Arrow RecordBatchReader following execute()", py::arg("rows_per_batch") = 1000000, py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "torch",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchPyTorch();
	    },
	    "Fetch a result as dict of PyTorch Tensors following execute()", py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "tf",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchTF();
	    },
	    "Fetch a result as dict of TensorFlow Tensors following execute()", py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "begin",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Begin();
	    },
	    "Start a new transaction", py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "commit",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Commit();
	    },
	    "Commit changes performed within a transaction", py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "rollback",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Rollback();
	    },
	    "Roll back changes performed within a transaction", py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "checkpoint",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Checkpoint();
	    },
	    "Synchronizes data in the write-ahead log (WAL) to the database data file (no-op for in-memory connections)",
	    py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "append",
	    [](const string &name, const PandasDataFrame &value, bool by_name,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Append(name, value, by_name);
	    },
	    "Append the passed DataFrame to the named table", py::arg("table_name"), py::arg("df"), py::kw_only(),
	    py::arg("by_name") = false, py::arg("conn") = py::none());
	m.def(
	    "register",
	    [](const string &name, const py::object &python_object, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->RegisterPythonObject(name, python_object);
	    },
	    "Register the passed Python Object value for querying with a view", py::arg("view_name"),
	    py::arg("python_object"), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "unregister",
	    [](const string &name, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->UnregisterPythonObject(name);
	    },
	    "Unregister the view name", py::arg("view_name"), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "table",
	    [](const string &tname, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Table(tname);
	    },
	    "Create a relation object for the named table", py::arg("table_name"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "view",
	    [](const string &vname, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->View(vname);
	    },
	    "Create a relation object for the named view", py::arg("view_name"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "values",
	    [](py::object params, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Values(params);
	    },
	    "Create a relation object from the passed values", py::arg("values"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "table_function",
	    [](const string &fname, py::object params, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->TableFunction(fname, params);
	    },
	    "Create a relation object from the named table function with given parameters", py::arg("name"),
	    py::arg("parameters") = py::none(), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "read_json",
	    [](const string &filename, const Optional<py::object> &columns, const Optional<py::object> &sample_size,
	       const Optional<py::object> &maximum_depth, const Optional<py::str> &records, const Optional<py::str> &format,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ReadJSON(filename, columns, sample_size, maximum_depth, records, format);
	    },
	    "Create a relation object from the JSON file in 'name'", py::arg("name"), py::kw_only(),
	    py::arg("columns") = py::none(), py::arg("sample_size") = py::none(), py::arg("maximum_depth") = py::none(),
	    py::arg("records") = py::none(), py::arg("format") = py::none(), py::arg("conn") = py::none());
	m.def(
	    "extract_statements",
	    [](const string &query, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ExtractStatements(query);
	    },
	    "Parse the query string and extract the Statement object(s) produced", py::arg("query"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "sql",
	    [](const py::object &query, string alias, const py::object &params,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->RunQuery(query, alias, params);
	    },
	    "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
	    "run the query as-is.",
	    py::arg("query"), py::kw_only(), py::arg("alias") = "", py::arg("params") = py::none(),
	    py::arg("conn") = py::none());
	m.def(
	    "query",
	    [](const py::object &query, string alias, const py::object &params,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->RunQuery(query, alias, params);
	    },
	    "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
	    "run the query as-is.",
	    py::arg("query"), py::kw_only(), py::arg("alias") = "", py::arg("params") = py::none(),
	    py::arg("conn") = py::none());
	m.def(
	    "from_query",
	    [](const py::object &query, string alias, const py::object &params,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->RunQuery(query, alias, params);
	    },
	    "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
	    "run the query as-is.",
	    py::arg("query"), py::kw_only(), py::arg("alias") = "", py::arg("params") = py::none(),
	    py::arg("conn") = py::none());
	m.def(
	    "read_csv",
	    [](const py::object &name, const py::object &header, const py::object &compression, const py::object &sep,
	       const py::object &delimiter, const py::object &dtype, const py::object &na_values,
	       const py::object &skiprows, const py::object &quotechar, const py::object &escapechar,
	       const py::object &encoding, const py::object &parallel, const py::object &date_format,
	       const py::object &timestamp_format, const py::object &sample_size, const py::object &all_varchar,
	       const py::object &normalize_names, const py::object &filename, const py::object &null_padding,
	       const py::object &names, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ReadCSV(name, header, compression, sep, delimiter, dtype, na_values, skiprows, quotechar,
		                         escapechar, encoding, parallel, date_format, timestamp_format, sample_size,
		                         all_varchar, normalize_names, filename, null_padding, names);
	    },
	    "Create a relation object from the CSV file in 'name'", py::arg("path_or_buffer"), py::kw_only(),
	    py::arg("header") = py::none(), py::arg("compression") = py::none(), py::arg("sep") = py::none(),
	    py::arg("delimiter") = py::none(), py::arg("dtype") = py::none(), py::arg("na_values") = py::none(),
	    py::arg("skiprows") = py::none(), py::arg("quotechar") = py::none(), py::arg("escapechar") = py::none(),
	    py::arg("encoding") = py::none(), py::arg("parallel") = py::none(), py::arg("date_format") = py::none(),
	    py::arg("timestamp_format") = py::none(), py::arg("sample_size") = py::none(),
	    py::arg("all_varchar") = py::none(), py::arg("normalize_names") = py::none(), py::arg("filename") = py::none(),
	    py::arg("null_padding") = py::none(), py::arg("names") = py::none(), py::arg("conn") = py::none());
	m.def(
	    "from_csv_auto",
	    [](const py::object &name, const py::object &header, const py::object &compression, const py::object &sep,
	       const py::object &delimiter, const py::object &dtype, const py::object &na_values,
	       const py::object &skiprows, const py::object &quotechar, const py::object &escapechar,
	       const py::object &encoding, const py::object &parallel, const py::object &date_format,
	       const py::object &timestamp_format, const py::object &sample_size, const py::object &all_varchar,
	       const py::object &normalize_names, const py::object &filename, const py::object &null_padding,
	       const py::object &names, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ReadCSV(name, header, compression, sep, delimiter, dtype, na_values, skiprows, quotechar,
		                         escapechar, encoding, parallel, date_format, timestamp_format, sample_size,
		                         all_varchar, normalize_names, filename, null_padding, names);
	    },
	    "Create a relation object from the CSV file in 'name'", py::arg("path_or_buffer"), py::kw_only(),
	    py::arg("header") = py::none(), py::arg("compression") = py::none(), py::arg("sep") = py::none(),
	    py::arg("delimiter") = py::none(), py::arg("dtype") = py::none(), py::arg("na_values") = py::none(),
	    py::arg("skiprows") = py::none(), py::arg("quotechar") = py::none(), py::arg("escapechar") = py::none(),
	    py::arg("encoding") = py::none(), py::arg("parallel") = py::none(), py::arg("date_format") = py::none(),
	    py::arg("timestamp_format") = py::none(), py::arg("sample_size") = py::none(),
	    py::arg("all_varchar") = py::none(), py::arg("normalize_names") = py::none(), py::arg("filename") = py::none(),
	    py::arg("null_padding") = py::none(), py::arg("names") = py::none(), py::arg("conn") = py::none());
	m.def(
	    "from_df",
	    [](const PandasDataFrame &value, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromDF(value);
	    },
	    "Create a relation object from the DataFrame in df", py::arg("df"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "from_arrow",
	    [](py::object &arrow_object, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromArrow(arrow_object);
	    },
	    "Create a relation object from an Arrow object", py::arg("arrow_object"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "from_parquet",
	    [](const string &file_glob, bool binary_as_string, bool file_row_number, bool filename, bool hive_partitioning,
	       bool union_by_name, const py::object &compression, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromParquet(file_glob, binary_as_string, file_row_number, filename, hive_partitioning,
		                             union_by_name, compression);
	    },
	    "Create a relation object from the Parquet files in file_glob", py::arg("file_glob"),
	    py::arg("binary_as_string") = false, py::kw_only(), py::arg("file_row_number") = false,
	    py::arg("filename") = false, py::arg("hive_partitioning") = false, py::arg("union_by_name") = false,
	    py::arg("compression") = py::none(), py::arg("conn") = py::none());
	m.def(
	    "read_parquet",
	    [](const string &file_glob, bool binary_as_string, bool file_row_number, bool filename, bool hive_partitioning,
	       bool union_by_name, const py::object &compression, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromParquet(file_glob, binary_as_string, file_row_number, filename, hive_partitioning,
		                             union_by_name, compression);
	    },
	    "Create a relation object from the Parquet files in file_glob", py::arg("file_glob"),
	    py::arg("binary_as_string") = false, py::kw_only(), py::arg("file_row_number") = false,
	    py::arg("filename") = false, py::arg("hive_partitioning") = false, py::arg("union_by_name") = false,
	    py::arg("compression") = py::none(), py::arg("conn") = py::none());
	m.def(
	    "from_parquet",
	    [](const vector<string> &file_globs, bool binary_as_string, bool file_row_number, bool filename,
	       bool hive_partitioning, bool union_by_name, const py::object &compression,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromParquets(file_globs, binary_as_string, file_row_number, filename, hive_partitioning,
		                              union_by_name, compression);
	    },
	    "Create a relation object from the Parquet files in file_globs", py::arg("file_globs"),
	    py::arg("binary_as_string") = false, py::kw_only(), py::arg("file_row_number") = false,
	    py::arg("filename") = false, py::arg("hive_partitioning") = false, py::arg("union_by_name") = false,
	    py::arg("compression") = py::none(), py::arg("conn") = py::none());
	m.def(
	    "read_parquet",
	    [](const vector<string> &file_globs, bool binary_as_string, bool file_row_number, bool filename,
	       bool hive_partitioning, bool union_by_name, const py::object &compression,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromParquets(file_globs, binary_as_string, file_row_number, filename, hive_partitioning,
		                              union_by_name, compression);
	    },
	    "Create a relation object from the Parquet files in file_globs", py::arg("file_globs"),
	    py::arg("binary_as_string") = false, py::kw_only(), py::arg("file_row_number") = false,
	    py::arg("filename") = false, py::arg("hive_partitioning") = false, py::arg("union_by_name") = false,
	    py::arg("compression") = py::none(), py::arg("conn") = py::none());
	m.def(
	    "from_substrait",
	    [](py::bytes &proto, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromSubstrait(proto);
	    },
	    "Create a query object from protobuf plan", py::arg("proto"), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "get_substrait",
	    [](const string &query, bool enable_optimizer = true, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->GetSubstrait(query, enable_optimizer);
	    },
	    "Serialize a query to protobuf", py::arg("query"), py::kw_only(), py::arg("enable_optimizer") = true,
	    py::arg("conn") = py::none());
	m.def(
	    "get_substrait_json",
	    [](const string &query, bool enable_optimizer = true, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->GetSubstraitJSON(query, enable_optimizer);
	    },
	    "Serialize a query to protobuf on the JSON format", py::arg("query"), py::kw_only(),
	    py::arg("enable_optimizer") = true, py::arg("conn") = py::none());
	m.def(
	    "from_substrait_json",
	    [](const string &json, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromSubstraitJSON(json);
	    },
	    "Create a query object from a JSON protobuf plan", py::arg("json"), py::kw_only(),
	    py::arg("conn") = py::none());
	m.def(
	    "get_table_names",
	    [](const string &query, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->GetTableNames(query);
	    },
	    "Extract the required table names from a query", py::arg("query"), py::kw_only(), py::arg("conn") = py::none());
	m.def(
	    "install_extension",
	    [](const string &extension, bool force_install = false, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->InstallExtension(extension, force_install);
	    },
	    "Install an extension by name", py::arg("extension"), py::kw_only(), py::arg("force_install") = false,
	    py::arg("conn") = py::none());
	m.def(
	    "load_extension",
	    [](const string &extension, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->LoadExtension(extension);
	    },
	    "Load an installed extension", py::arg("extension"), py::kw_only(), py::arg("conn") = py::none());
	// END_OF_CONNECTION_METHODS

	// We define these "wrapper" methods manually because they are overloaded
	m.def(
	    "arrow",
	    [](idx_t rows_per_batch, shared_ptr<DuckDBPyConnection> conn) -> duckdb::pyarrow::Table {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchArrow(rows_per_batch);
	    },
	    "Fetch a result as Arrow table following execute()", py::arg("rows_per_batch") = 1000000, py::kw_only(),
	    py::arg("connection") = py::none());
	m.def(
	    "arrow",
	    [](py::object &arrow_object, shared_ptr<DuckDBPyConnection> conn) -> unique_ptr<DuckDBPyRelation> {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromArrow(arrow_object);
	    },
	    "Create a relation object from an Arrow object", py::arg("arrow_object"), py::kw_only(),
	    py::arg("connection") = py::none());
	m.def(
	    "df",
	    [](bool date_as_object, shared_ptr<DuckDBPyConnection> conn) -> PandasDataFrame {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchDF(date_as_object);
	    },
	    "Fetch a result as DataFrame following execute()", py::kw_only(), py::arg("date_as_object") = false,
	    py::arg("connection") = py::none());
	m.def(
	    "df",
	    [](const PandasDataFrame &value, shared_ptr<DuckDBPyConnection> conn) -> unique_ptr<DuckDBPyRelation> {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromDF(value);
	    },
	    "Create a relation object from the DataFrame df", py::arg("df"), py::kw_only(),
	    py::arg("connection") = py::none());
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
