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
#include "duckdb/common/unordered_map.hpp"
#include "duckdb_python/import_cache/python_import_cache.hpp"
#include "duckdb_python/registered_py_object.hpp"
#include "duckdb_python/pandas_type.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb/execution/operator/persistent/csv_reader_options.hpp"
#include "duckdb_python/pyfilesystem.hpp"

namespace duckdb {

enum class PythonEnvironmentType { NORMAL, INTERACTIVE, JUPYTER };

struct DuckDBPyRelation;

class RegisteredArrow : public RegisteredObject {

public:
	RegisteredArrow(duckdb::unique_ptr<PythonTableArrowArrayStreamFactory> arrow_factory_p, py::object obj_p)
	    : RegisteredObject(std::move(obj_p)), arrow_factory(std::move(arrow_factory_p)) {};
	duckdb::unique_ptr<PythonTableArrowArrayStreamFactory> arrow_factory;
};

struct DuckDBPyConnection : public std::enable_shared_from_this<DuckDBPyConnection> {
public:
	shared_ptr<DuckDB> database;
	duckdb::unique_ptr<Connection> connection;
	duckdb::unique_ptr<DuckDBPyRelation> result;
	vector<shared_ptr<DuckDBPyConnection>> cursors;
	unordered_map<string, shared_ptr<Relation>> temporary_views;
	std::mutex py_connection_lock;

public:
	explicit DuckDBPyConnection() {
	}

public:
	static void Initialize(py::handle &m);
	static void Cleanup();

	shared_ptr<DuckDBPyConnection> Enter();

	static bool Exit(DuckDBPyConnection &self, const py::object &exc_type, const py::object &exc,
	                 const py::object &traceback);

	static bool DetectAndGetEnvironment();
	static bool IsJupyter();
	static shared_ptr<DuckDBPyConnection> DefaultConnection();
	static PythonImportCache *ImportCache();
	static bool IsInteractive();

	duckdb::unique_ptr<DuckDBPyRelation>
	ReadCSV(const string &name, const py::object &header = py::none(), const py::object &compression = py::none(),
	        const py::object &sep = py::none(), const py::object &delimiter = py::none(),
	        const py::object &dtype = py::none(), const py::object &na_values = py::none(),
	        const py::object &skiprows = py::none(), const py::object &quotechar = py::none(),
	        const py::object &escapechar = py::none(), const py::object &encoding = py::none(),
	        const py::object &parallel = py::none(), const py::object &date_format = py::none(),
	        const py::object &timestamp_format = py::none(), const py::object &sample_size = py::none(),
	        const py::object &all_varchar = py::none(), const py::object &normalize_names = py::none(),
	        const py::object &filename = py::none());

	duckdb::unique_ptr<DuckDBPyRelation> ReadJSON(const string &filename, const py::object &columns = py::none(),
	                                              const py::object &sample_size = py::none(),
	                                              const py::object &maximum_depth = py::none());

	shared_ptr<DuckDBPyConnection> ExecuteMany(const string &query, py::object params = py::list());

	duckdb::unique_ptr<QueryResult> ExecuteInternal(const string &query, py::object params = py::list(),
	                                                bool many = false);

	shared_ptr<DuckDBPyConnection> Execute(const string &query, py::object params = py::list(), bool many = false);

	shared_ptr<DuckDBPyConnection> Append(const string &name, DataFrame value);

	shared_ptr<DuckDBPyConnection> RegisterPythonObject(const string &name, py::object python_object);

	void InstallExtension(const string &extension, bool force_install = false);

	void LoadExtension(const string &extension);

	duckdb::unique_ptr<DuckDBPyRelation> FromQuery(const string &query, const string &alias = "query_relation");
	duckdb::unique_ptr<DuckDBPyRelation> RunQuery(const string &query, const string &alias = "query_relation");

	duckdb::unique_ptr<DuckDBPyRelation> Table(const string &tname);

	duckdb::unique_ptr<DuckDBPyRelation> Values(py::object params = py::none());

	duckdb::unique_ptr<DuckDBPyRelation> View(const string &vname);

	duckdb::unique_ptr<DuckDBPyRelation> TableFunction(const string &fname, py::object params = py::list());

	duckdb::unique_ptr<DuckDBPyRelation> FromDF(const DataFrame &value);

	duckdb::unique_ptr<DuckDBPyRelation> FromParquet(const string &file_glob, bool binary_as_string,
	                                                 bool file_row_number, bool filename, bool hive_partitioning,
	                                                 bool union_by_name, const py::object &compression = py::none());

	duckdb::unique_ptr<DuckDBPyRelation> FromParquets(const vector<string> &file_globs, bool binary_as_string,
	                                                  bool file_row_number, bool filename, bool hive_partitioning,
	                                                  bool union_by_name, const py::object &compression = py::none());

	duckdb::unique_ptr<DuckDBPyRelation> FromArrow(py::object &arrow_object);

	duckdb::unique_ptr<DuckDBPyRelation> FromSubstrait(py::bytes &proto);

	duckdb::unique_ptr<DuckDBPyRelation> GetSubstrait(const string &query, bool enable_optimizer = true);

	duckdb::unique_ptr<DuckDBPyRelation> GetSubstraitJSON(const string &query, bool enable_optimizer = true);

	duckdb::unique_ptr<DuckDBPyRelation> FromSubstraitJSON(const string &json);

	unordered_set<string> GetTableNames(const string &query);

	shared_ptr<DuckDBPyConnection> UnregisterPythonObject(const string &name);

	shared_ptr<DuckDBPyConnection> Begin();

	shared_ptr<DuckDBPyConnection> Commit();

	shared_ptr<DuckDBPyConnection> Rollback();

	void Close();

	// cursor() is stupid
	shared_ptr<DuckDBPyConnection> Cursor();

	Optional<py::list> GetDescription();

	// these should be functions on the result but well
	Optional<py::tuple> FetchOne();

	py::list FetchMany(idx_t size);

	py::list FetchAll();

	py::dict FetchNumpy();
	DataFrame FetchDF(bool date_as_object);
	DataFrame FetchDFChunk(const idx_t vectors_per_chunk = 1, bool date_as_object = false) const;

	duckdb::pyarrow::Table FetchArrow(idx_t chunk_size);
	PolarsDataFrame FetchPolars(idx_t chunk_size);

	py::dict FetchPyTorch();

	py::dict FetchTF();

	duckdb::pyarrow::RecordBatchReader FetchRecordBatchReader(const idx_t chunk_size) const;

	static shared_ptr<DuckDBPyConnection> Connect(const string &database, bool read_only, const py::dict &config);

	static vector<Value> TransformPythonParamList(const py::handle &params);

	void RegisterFilesystem(AbstractFileSystem filesystem);
	void UnregisterFilesystem(const py::str &name);
	py::list ListFilesystems();

	//! Default connection to an in-memory database
	static shared_ptr<DuckDBPyConnection> default_connection;
	//! Caches and provides an interface to get frequently used modules+subtypes
	static shared_ptr<PythonImportCache> import_cache;

	static bool IsPandasDataframe(const py::object &object);
	static bool IsAcceptedArrowObject(const py::object &object);

	static duckdb::unique_ptr<QueryResult> CompletePendingQuery(PendingQueryResult &pending_query);

private:
	unique_lock<std::mutex> AcquireConnectionLock();
	static PythonEnvironmentType environment;
	static void DetectEnvironment();
};

template <class T>
static bool ModuleIsLoaded() {
	auto dict = pybind11::module_::import("sys").attr("modules");
	return dict.contains(py::str(T::Name));
}

} // namespace duckdb
