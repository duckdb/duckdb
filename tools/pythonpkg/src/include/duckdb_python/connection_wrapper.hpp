#pragma once

#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/python_objects.hpp"

namespace duckdb {

class PyConnectionWrapper {
public:
	PyConnectionWrapper() = delete;

public:
	static shared_ptr<DuckDBPyConnection> ExecuteMany(const string &query, py::object params = py::list(),
	                                                  shared_ptr<DuckDBPyConnection> conn = nullptr);

	static shared_ptr<DuckDBPyConnection> Execute(const string &query, py::object params = py::list(),
	                                              bool many = false, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static shared_ptr<DuckDBPyConnection> Append(const string &name, DataFrame value,
	                                             shared_ptr<DuckDBPyConnection> conn = nullptr);

	static shared_ptr<DuckDBPyConnection> RegisterPythonObject(const string &name, py::object python_object,
	                                                           shared_ptr<DuckDBPyConnection> conn = nullptr);

	static void InstallExtension(const string &extension, bool force_install = false,
	                             shared_ptr<DuckDBPyConnection> conn = nullptr);

	static void LoadExtension(const string &extension, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromQuery(const string &query, const string &alias = "query_relation",
	                                              shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> RunQuery(const string &query, const string &alias = "query_relation",
	                                             shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> Table(const string &tname, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> Values(py::object params = py::none(),
	                                           shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> View(const string &vname, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> TableFunction(const string &fname, py::object params = py::list(),
	                                                  shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromDF(const DataFrame &value, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromParquet(const string &file_glob, bool binary_as_string,
	                                                bool file_row_number, bool filename, bool hive_partitioning,
	                                                bool union_by_name, const py::object &compression = py::none(),
	                                                shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromParquets(const vector<string> &file_globs, bool binary_as_string,
	                                                 bool file_row_number, bool filename, bool hive_partitioning,
	                                                 bool union_by_name, const py::object &compression = py::none(),
	                                                 shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromArrow(py::object &arrow_object,
	                                              shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromSubstrait(py::bytes &proto, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> GetSubstrait(const string &query,
	                                                 shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> GetSubstraitJSON(const string &query,
	                                                     shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unordered_set<string> GetTableNames(const string &query, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static shared_ptr<DuckDBPyConnection> UnregisterPythonObject(const string &name,
	                                                             shared_ptr<DuckDBPyConnection> conn = nullptr);

	static shared_ptr<DuckDBPyConnection> Begin(shared_ptr<DuckDBPyConnection> conn = nullptr);

	static shared_ptr<DuckDBPyConnection> Commit(shared_ptr<DuckDBPyConnection> conn = nullptr);

	static shared_ptr<DuckDBPyConnection> Rollback(shared_ptr<DuckDBPyConnection> conn = nullptr);

	static void Close(shared_ptr<DuckDBPyConnection> conn = nullptr);

	static shared_ptr<DuckDBPyConnection> Cursor(shared_ptr<DuckDBPyConnection> conn = nullptr);

	static Optional<py::list> GetDescription(shared_ptr<DuckDBPyConnection> conn = nullptr);

	static Optional<py::tuple> FetchOne(shared_ptr<DuckDBPyConnection> conn = nullptr);

	static py::list FetchMany(idx_t size, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> ReadJSON(const string &filename, shared_ptr<DuckDBPyConnection> conn = nullptr,
	                                             const py::object &columns = py::none(),
	                                             const py::object &sample_size = py::none(),
	                                             const py::object &maximum_depth = py::none());
	static unique_ptr<DuckDBPyRelation>
	ReadCSV(const string &name, shared_ptr<DuckDBPyConnection> conn, const py::object &header = py::none(),
	        const py::object &compression = py::none(), const py::object &sep = py::none(),
	        const py::object &delimiter = py::none(), const py::object &dtype = py::none(),
	        const py::object &na_values = py::none(), const py::object &skiprows = py::none(),
	        const py::object &quotechar = py::none(), const py::object &escapechar = py::none(),
	        const py::object &encoding = py::none(), const py::object &parallel = py::none(),
	        const py::object &date_format = py::none(), const py::object &timestamp_format = py::none(),
	        const py::object &sample_size = py::none(), const py::object &all_varchar = py::none(),
	        const py::object &normalize_names = py::none(), const py::object &filename = py::none());

	static py::list FetchAll(shared_ptr<DuckDBPyConnection> conn = nullptr);

	static py::dict FetchNumpy(shared_ptr<DuckDBPyConnection> conn = nullptr);

	static DataFrame FetchDF(bool date_as_object, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static DataFrame FetchDFChunk(const idx_t vectors_per_chunk = 1, bool date_as_object = false,
	                              shared_ptr<DuckDBPyConnection> conn = nullptr);

	static duckdb::pyarrow::Table FetchArrow(idx_t chunk_size, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static duckdb::pyarrow::RecordBatchReader FetchRecordBatchReader(const idx_t chunk_size,
	                                                                 shared_ptr<DuckDBPyConnection> conn = nullptr);

	static PolarsDataFrame FetchPolars(idx_t chunk_size, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static void RegisterFilesystem(AbstractFileSystem file_system, shared_ptr<DuckDBPyConnection> conn);
	static void UnregisterFilesystem(const py::str &name, shared_ptr<DuckDBPyConnection> conn);
	static py::list ListFilesystems(shared_ptr<DuckDBPyConnection> conn);
};
} // namespace duckdb
