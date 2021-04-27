//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyconnection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "arrow_array_stream.hpp"
#include "duckdb.hpp"
#include "duckdb_python/pybind_wrapper.hpp"

namespace duckdb {

struct DuckDBPyRelation;
struct DuckDBPyResult;

struct DuckDBPyConnection {
public:
	~DuckDBPyConnection();

	shared_ptr<DuckDB> database;
	unique_ptr<Connection> connection;
	unordered_map<string, py::object> registered_dfs;
	unordered_map<string, unique_ptr<PythonTableArrowArrayStreamFactory>> registered_arrow_factory;
	unique_ptr<DuckDBPyResult> result;
	vector<shared_ptr<DuckDBPyConnection>> cursors;

public:
	static void Initialize(py::handle &m);
	static void Cleanup();

	static DuckDBPyConnection *DefaultConnection();

	DuckDBPyConnection *ExecuteMany(const string &query, py::object params = py::list());

	DuckDBPyConnection *Execute(const string &query, py::object params = py::list(), bool many = false);

	DuckDBPyConnection *Append(const string &name, py::object value);

	DuckDBPyConnection *RegisterDF(const string &name, py::object value);

	unique_ptr<DuckDBPyRelation> FromQuery(const string &query, const string &alias = "query_relation");
	DuckDBPyConnection *RegisterArrow(const string &name, const py::object &value);

	unique_ptr<DuckDBPyRelation> Table(const string &tname);

	unique_ptr<DuckDBPyRelation> Values(py::object params = py::list());

	unique_ptr<DuckDBPyRelation> View(const string &vname);

	unique_ptr<DuckDBPyRelation> TableFunction(const string &fname, py::object params = py::list());

	unique_ptr<DuckDBPyRelation> FromDF(py::object value);

	unique_ptr<DuckDBPyRelation> FromCsvAuto(const string &filename);

	unique_ptr<DuckDBPyRelation> FromParquet(const string &filename);

	unique_ptr<DuckDBPyRelation> FromArrowTable(const py::object &table);

	DuckDBPyConnection *UnregisterDF(const string &name);

	DuckDBPyConnection *UnregisterArrow(const string &name);

	DuckDBPyConnection *Begin();

	DuckDBPyConnection *Commit();

	DuckDBPyConnection *Rollback();

	py::object GetAttr(const py::str &key);

	void Close();

	// cursor() is stupid
	shared_ptr<DuckDBPyConnection> Cursor();

	// these should be functions on the result but well
	py::object FetchOne();

	py::list FetchAll();

	py::dict FetchNumpy();
	py::object FetchDF();

	py::object FetchDFChunk() const;

	py::object FetchArrow();

	static shared_ptr<DuckDBPyConnection> Connect(const string &database, bool read_only);

	static vector<Value> TransformPythonParamList(py::handle params);

private:
	//! Default connection to an in-memory database
	static shared_ptr<DuckDBPyConnection> default_connection;
};

} // namespace duckdb
