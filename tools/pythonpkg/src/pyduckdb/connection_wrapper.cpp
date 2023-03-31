#include "duckdb_python/connection_wrapper.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::ExecuteMany(const string &query, py::object params,
                                                                shared_ptr<DuckDBPyConnection> conn) {
	return conn->ExecuteMany(query, params);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::DistinctDF(const DataFrame &df, shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->Distinct();
}

void PyConnectionWrapper::WriteCsvDF(const DataFrame &df, const string &file, shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->ToCSV(file);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::QueryDF(const DataFrame &df, const string &view_name,
                                                          const string &sql_query,
                                                          shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->Query(view_name, sql_query);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::AggregateDF(const DataFrame &df, const string &expr,
                                                              const string &groups,
                                                              shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->Aggregate(expr, groups);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Execute(const string &query, py::object params, bool many,
                                                            shared_ptr<DuckDBPyConnection> conn) {
	return conn->Execute(query, params, many);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Append(const string &name, DataFrame value,
                                                           shared_ptr<DuckDBPyConnection> conn) {
	return conn->Append(name, value);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::RegisterPythonObject(const string &name, py::object python_object,
                                                                         shared_ptr<DuckDBPyConnection> conn) {
	return conn->RegisterPythonObject(name, python_object);
}

void PyConnectionWrapper::InstallExtension(const string &extension, bool force_install,
                                           shared_ptr<DuckDBPyConnection> conn) {
	conn->InstallExtension(extension, force_install);
}

void PyConnectionWrapper::LoadExtension(const string &extension, shared_ptr<DuckDBPyConnection> conn) {
	conn->LoadExtension(extension);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::Table(const string &tname, shared_ptr<DuckDBPyConnection> conn) {
	return conn->Table(tname);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::View(const string &vname, shared_ptr<DuckDBPyConnection> conn) {
	return conn->View(vname);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::TableFunction(const string &fname, py::object params,
                                                                shared_ptr<DuckDBPyConnection> conn) {
	return conn->TableFunction(fname, params);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromDF(const DataFrame &value, shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(value);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromParquet(const string &file_glob, bool binary_as_string,
                                                              bool file_row_number, bool filename,
                                                              bool hive_partitioning, bool union_by_name,
                                                              const py::object &compression,
                                                              shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromParquet(file_glob, binary_as_string, file_row_number, filename, hive_partitioning, union_by_name,
	                         compression);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromParquets(const vector<string> &file_globs, bool binary_as_string,
                                                               bool file_row_number, bool filename,
                                                               bool hive_partitioning, bool union_by_name,
                                                               const py::object &compression,
                                                               shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromParquets(file_globs, binary_as_string, file_row_number, filename, hive_partitioning, union_by_name,
	                          compression);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromArrow(py::object &arrow_object,
                                                            shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromArrow(arrow_object);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromSubstrait(py::bytes &proto, shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromSubstrait(proto);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromSubstraitJSON(const string &json,
                                                                    shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromSubstraitJSON(json);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::GetSubstrait(const string &query, shared_ptr<DuckDBPyConnection> conn,
                                                               bool enable_optimizer) {
	return conn->GetSubstrait(query, enable_optimizer);
}

unique_ptr<DuckDBPyRelation>
PyConnectionWrapper::GetSubstraitJSON(const string &query, shared_ptr<DuckDBPyConnection> conn, bool enable_optimizer) {
	return conn->GetSubstraitJSON(query, enable_optimizer);
}

unordered_set<string> PyConnectionWrapper::GetTableNames(const string &query, shared_ptr<DuckDBPyConnection> conn) {
	return conn->GetTableNames(query);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::UnregisterPythonObject(const string &name,
                                                                           shared_ptr<DuckDBPyConnection> conn) {
	return conn->UnregisterPythonObject(name);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Begin(shared_ptr<DuckDBPyConnection> conn) {
	return conn->Begin();
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Commit(shared_ptr<DuckDBPyConnection> conn) {
	return conn->Commit();
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Rollback(shared_ptr<DuckDBPyConnection> conn) {
	return conn->Rollback();
}

void PyConnectionWrapper::Close(shared_ptr<DuckDBPyConnection> conn) {
	conn->Close();
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Cursor(shared_ptr<DuckDBPyConnection> conn) {
	return conn->Cursor();
}

Optional<py::list> PyConnectionWrapper::GetDescription(shared_ptr<DuckDBPyConnection> conn) {
	return conn->GetDescription();
}

Optional<py::tuple> PyConnectionWrapper::FetchOne(shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchOne();
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::ReadJSON(const string &filename, shared_ptr<DuckDBPyConnection> conn,
                                                           const py::object &columns, const py::object &sample_size,
                                                           const py::object &maximum_depth) {

	return conn->ReadJSON(filename, columns, sample_size, maximum_depth);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::ReadCSV(
    const py::object &name, shared_ptr<DuckDBPyConnection> conn, const py::object &header,
    const py::object &compression, const py::object &sep, const py::object &delimiter, const py::object &dtype,
    const py::object &na_values, const py::object &skiprows, const py::object &quotechar, const py::object &escapechar,
    const py::object &encoding, const py::object &parallel, const py::object &date_format,
    const py::object &timestamp_format, const py::object &sample_size, const py::object &all_varchar,
    const py::object &normalize_names, const py::object &filename) {
	return conn->ReadCSV(name, header, compression, sep, delimiter, dtype, na_values, skiprows, quotechar, escapechar,
	                     encoding, parallel, date_format, timestamp_format, sample_size, all_varchar, normalize_names,
	                     filename);
}

py::list PyConnectionWrapper::FetchMany(idx_t size, shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchMany(size);
}

py::list PyConnectionWrapper::FetchAll(shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchAll();
}

py::dict PyConnectionWrapper::FetchNumpy(shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchNumpy();
}

DataFrame PyConnectionWrapper::FetchDF(bool date_as_object, shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchDF(date_as_object);
}

DataFrame PyConnectionWrapper::FetchDFChunk(const idx_t vectors_per_chunk, bool date_as_object,
                                            shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchDFChunk(vectors_per_chunk, date_as_object);
}

duckdb::pyarrow::Table PyConnectionWrapper::FetchArrow(idx_t chunk_size, shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchArrow(chunk_size);
}

py::dict PyConnectionWrapper::FetchPyTorch(shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchPyTorch();
}

py::dict PyConnectionWrapper::FetchTF(shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchTF();
}

PolarsDataFrame PyConnectionWrapper::FetchPolars(idx_t chunk_size, shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchPolars(chunk_size);
}

duckdb::pyarrow::RecordBatchReader PyConnectionWrapper::FetchRecordBatchReader(const idx_t chunk_size,
                                                                               shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchRecordBatchReader(chunk_size);
}

void PyConnectionWrapper::RegisterFilesystem(AbstractFileSystem file_system, shared_ptr<DuckDBPyConnection> conn) {
	return conn->RegisterFilesystem(std::move(file_system));
}
void PyConnectionWrapper::UnregisterFilesystem(const py::str &name, shared_ptr<DuckDBPyConnection> conn) {
	return conn->UnregisterFilesystem(name);
}
py::list PyConnectionWrapper::ListFilesystems(shared_ptr<DuckDBPyConnection> conn) {
	return conn->ListFilesystems();
}
bool PyConnectionWrapper::FileSystemIsRegistered(const string &name, shared_ptr<DuckDBPyConnection> conn) {
	return conn->FileSystemIsRegistered(name);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::Values(py::object values, shared_ptr<DuckDBPyConnection> conn) {
	return conn->Values(std::move(values));
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromQuery(const string &query, const string &alias,
                                                            shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromQuery(query, alias);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::RunQuery(const string &query, const string &alias,
                                                           shared_ptr<DuckDBPyConnection> conn) {
	return conn->RunQuery(query, alias);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::ProjectDf(const DataFrame &df, const string &expr,
                                                            shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->Project(expr);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::AliasDF(const DataFrame &df, const string &expr,
                                                          shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->SetAlias(expr);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FilterDf(const DataFrame &df, const string &expr,
                                                           shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->Filter(expr);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::LimitDF(const DataFrame &df, int64_t n,
                                                          shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->Limit(n);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::OrderDf(const DataFrame &df, const string &expr,
                                                          shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->Order(expr);
}

} // namespace duckdb
