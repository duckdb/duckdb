#include "duckdb_python/connection_wrapper.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {

shared_ptr<DuckDBPyType> PyConnectionWrapper::UnionType(const py::object &members,
                                                        shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->UnionType(members);
}

shared_ptr<DuckDBPyType> PyConnectionWrapper::EnumType(const string &name, const shared_ptr<DuckDBPyType> &type,
                                                       const py::list &values, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->EnumType(name, type, values);
}

shared_ptr<DuckDBPyType> PyConnectionWrapper::DecimalType(int width, int scale, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->DecimalType(width, scale);
}

shared_ptr<DuckDBPyType> PyConnectionWrapper::StringType(const string &collation, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->StringType(collation);
}

shared_ptr<DuckDBPyType> PyConnectionWrapper::ArrayType(const shared_ptr<DuckDBPyType> &type,
                                                        shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->ArrayType(type);
}

shared_ptr<DuckDBPyType> PyConnectionWrapper::MapType(const shared_ptr<DuckDBPyType> &key,
                                                      const shared_ptr<DuckDBPyType> &value,
                                                      shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->MapType(key, value);
}

shared_ptr<DuckDBPyType> PyConnectionWrapper::StructType(const py::object &fields,
                                                         shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->StructType(fields);
}

shared_ptr<DuckDBPyType> PyConnectionWrapper::Type(const string &type_str, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->Type(type_str);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::ExecuteMany(const string &query, py::object params,
                                                                shared_ptr<DuckDBPyConnection> conn) {
	return conn->ExecuteMany(query, params);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::DistinctDF(const PandasDataFrame &df,
                                                             shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->Distinct();
}

void PyConnectionWrapper::WriteCsvDF(const PandasDataFrame &df, const string &file,
                                     shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->ToCSV(file);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::QueryDF(const PandasDataFrame &df, const string &view_name,
                                                          const string &sql_query,
                                                          shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->Query(view_name, sql_query);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::AggregateDF(const PandasDataFrame &df, const string &expr,
                                                              const string &groups,
                                                              shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->Aggregate(expr, groups);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Execute(const string &query, py::object params, bool many,
                                                            shared_ptr<DuckDBPyConnection> conn) {
	return conn->Execute(query, params, many);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::UnregisterUDF(const string &name,
                                                                  shared_ptr<DuckDBPyConnection> conn) {
	return conn->UnregisterUDF(name);
}

shared_ptr<DuckDBPyConnection>
PyConnectionWrapper::RegisterScalarUDF(const string &name, const py::function &udf, const py::object &parameters_p,
                                       const shared_ptr<DuckDBPyType> &return_type_p, PythonUDFType type,
                                       FunctionNullHandling null_handling, PythonExceptionHandling exception_handling,
                                       bool side_effects, shared_ptr<DuckDBPyConnection> conn) {
	return conn->RegisterScalarUDF(name, udf, parameters_p, return_type_p, type, null_handling, exception_handling,
	                               side_effects);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Append(const string &name, PandasDataFrame value, bool by_name,
                                                           shared_ptr<DuckDBPyConnection> conn) {
	return conn->Append(name, value, by_name);
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

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromDF(const PandasDataFrame &value,
                                                         shared_ptr<DuckDBPyConnection> conn) {
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

void PyConnectionWrapper::Interrupt(shared_ptr<DuckDBPyConnection> conn) {
	conn->Interrupt();
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
                                                           const Optional<py::object> &columns,
                                                           const Optional<py::object> &sample_size,
                                                           const Optional<py::object> &maximum_depth,
                                                           const Optional<py::str> &records,
                                                           const Optional<py::str> &format) {

	return conn->ReadJSON(filename, columns, sample_size, maximum_depth, records, format);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::ReadCSV(
    const py::object &name, shared_ptr<DuckDBPyConnection> conn, const py::object &header,
    const py::object &compression, const py::object &sep, const py::object &delimiter, const py::object &dtype,
    const py::object &na_values, const py::object &skiprows, const py::object &quotechar, const py::object &escapechar,
    const py::object &encoding, const py::object &parallel, const py::object &date_format,
    const py::object &timestamp_format, const py::object &sample_size, const py::object &all_varchar,
    const py::object &normalize_names, const py::object &filename, const py::object &null_padding) {
	return conn->ReadCSV(name, header, compression, sep, delimiter, dtype, na_values, skiprows, quotechar, escapechar,
	                     encoding, parallel, date_format, timestamp_format, sample_size, all_varchar, normalize_names,
	                     filename, null_padding);
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

PandasDataFrame PyConnectionWrapper::FetchDF(bool date_as_object, shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchDF(date_as_object);
}

PandasDataFrame PyConnectionWrapper::FetchDFChunk(const idx_t vectors_per_chunk, bool date_as_object,
                                                  shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchDFChunk(vectors_per_chunk, date_as_object);
}

duckdb::pyarrow::Table PyConnectionWrapper::FetchArrow(idx_t rows_per_batch, shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchArrow(rows_per_batch);
}

py::dict PyConnectionWrapper::FetchPyTorch(shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchPyTorch();
}

py::dict PyConnectionWrapper::FetchTF(shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchTF();
}

PolarsDataFrame PyConnectionWrapper::FetchPolars(idx_t rows_per_batch, shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchPolars(rows_per_batch);
}

duckdb::pyarrow::RecordBatchReader PyConnectionWrapper::FetchRecordBatchReader(const idx_t rows_per_batch,
                                                                               shared_ptr<DuckDBPyConnection> conn) {
	return conn->FetchRecordBatchReader(rows_per_batch);
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

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::RunQuery(const string &query, const string &alias,
                                                           shared_ptr<DuckDBPyConnection> conn) {
	return conn->RunQuery(query, alias);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::ProjectDf(const PandasDataFrame &df, const py::object &expr,
                                                            shared_ptr<DuckDBPyConnection> conn) {
	// FIXME: if we want to support passing in DuckDBPyExpressions here
	// we could also accept 'expr' as a List[DuckDBPyExpression], without changing the signature
	if (!py::isinstance<py::str>(expr)) {
		throw InvalidInputException("Please provide 'expr' as a string");
	}
	return conn->FromDF(df)->Project(expr);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::AliasDF(const PandasDataFrame &df, const string &expr,
                                                          shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->SetAlias(expr);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FilterDf(const PandasDataFrame &df, const string &expr,
                                                           shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->FilterFromExpression(expr);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::LimitDF(const PandasDataFrame &df, int64_t n,
                                                          shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->Limit(n);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::OrderDf(const PandasDataFrame &df, const string &expr,
                                                          shared_ptr<DuckDBPyConnection> conn) {
	return conn->FromDF(df)->Order(expr);
}

} // namespace duckdb
