//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyconnection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>

#include "arrow_array_stream.hpp"
#include "duckdb.hpp"
#include "duckdb_python/pybind_wrapper.hpp"
#include <thread>

namespace duckdb {

struct DuckDBPyRelation;
struct DuckDBPyResult;

class RegisteredObject {
public:
	explicit RegisteredObject(py::object obj_p) : obj(move(obj_p)) {
	}
	virtual ~RegisteredObject() {
		obj = py::none();
	}

	py::object obj;
};

class RegisteredArrow : public RegisteredObject {

public:
	RegisteredArrow(unique_ptr<PythonTableArrowArrayStreamFactory> arrow_factory_p, py::object obj_p)
	    : RegisteredObject(std::move(obj_p)), arrow_factory(move(arrow_factory_p)) {};
	unique_ptr<PythonTableArrowArrayStreamFactory> arrow_factory;
};

struct DuckDBPyConnection {
public:
	shared_ptr<DuckDB> database;
	unique_ptr<Connection> connection;
	unordered_map<string, unique_ptr<RegisteredObject>> registered_objects;
	unique_ptr<DuckDBPyResult> result;
	vector<shared_ptr<DuckDBPyConnection>> cursors;
	std::thread::id thread_id = std::this_thread::get_id();

public:
	static void Initialize(py::handle &m);
	static void Cleanup();

	static DuckDBPyConnection *DefaultConnection();

	DuckDBPyConnection *ExecuteMany(const string &query, py::object params = py::list());

	DuckDBPyConnection *Execute(const string &query, py::object params = py::list(), bool many = false);

	DuckDBPyConnection *Append(const string &name, py::object value);

	DuckDBPyConnection *RegisterPythonObject(const string &name, py::object python_object,
	                                         const idx_t rows_per_tuple = 100000);

	unique_ptr<DuckDBPyRelation> FromQuery(const string &query, const string &alias = "query_relation");

	unique_ptr<DuckDBPyRelation> Table(const string &tname);

	unique_ptr<DuckDBPyRelation> Values(py::object params = py::list());

	unique_ptr<DuckDBPyRelation> View(const string &vname);

	unique_ptr<DuckDBPyRelation> TableFunction(const string &fname, py::object params = py::list());

	unique_ptr<DuckDBPyRelation> FromDF(py::object value);

	unique_ptr<DuckDBPyRelation> FromCsvAuto(const string &filename);

	unique_ptr<DuckDBPyRelation> FromParquet(const string &filename, bool binary_as_string);

	unique_ptr<DuckDBPyRelation> FromArrowTable(py::object &table, const idx_t rows_per_tuple = 1000000);

	DuckDBPyConnection *UnregisterPythonObject(const string &name);

	DuckDBPyConnection *Begin();

	DuckDBPyConnection *Commit();

	DuckDBPyConnection *Rollback();

	void Close();

	// cursor() is stupid
	shared_ptr<DuckDBPyConnection> Cursor();

	py::object GetDescription();

	// these should be functions on the result but well
	py::object FetchOne();

	py::list FetchAll();

	py::dict FetchNumpy();
	py::object FetchDF();

	py::object FetchDFChunk(const idx_t vectors_per_chunk = 1) const;

	py::object FetchArrow();

	py::object FetchArrowChunk(const idx_t vectors_per_chunk, bool return_table) const;

	py::object FetchRecordBatchReader(const idx_t vectors_per_chunk) const;

	static shared_ptr<DuckDBPyConnection> Connect(const string &database, bool read_only, const py::dict &config);

	static vector<Value> TransformPythonParamList(py::handle params);

	//! Default connection to an in-memory database
	static shared_ptr<DuckDBPyConnection> default_connection;
};

} // namespace duckdb
