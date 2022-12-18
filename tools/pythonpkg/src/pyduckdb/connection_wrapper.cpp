#include "duckdb_python/connection_wrapper.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::ExecuteMany(const string &query, py::object params,
                                                                shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->ExecuteMany(query, params);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Execute(const string &query, py::object params, bool many,
                                                            shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->Execute(query, params, many);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Append(const string &name, DataFrame value,
                                                           shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->Append(name, value);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::RegisterPythonObject(const string &name, py::object python_object,
                                                                         shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->RegisterPythonObject(name, python_object);
}

void PyConnectionWrapper::InstallExtension(const string &extension, bool force_install,
                                           shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	conn->InstallExtension(extension, force_install);
}

void PyConnectionWrapper::LoadExtension(const string &extension, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	conn->LoadExtension(extension);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromQuery(const string &query, const string &alias,
                                                            shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromQuery(query, alias);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::RunQuery(const string &query, const string &alias,
                                                           shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->RunQuery(query, alias);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::Table(const string &tname, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->Table(tname);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::Values(py::object params, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->Values(params);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::View(const string &vname, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->View(vname);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::TableFunction(const string &fname, py::object params,
                                                                shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->TableFunction(fname, params);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromDF(const DataFrame &value, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromDF(value);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromCsvAuto(const string &filename,
                                                              shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromCsvAuto(filename);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromParquet(const string &file_glob, bool binary_as_string,
                                                              bool file_row_number, bool filename,
                                                              bool hive_partitioning,
                                                              shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromParquet(file_glob, binary_as_string, file_row_number, filename, hive_partitioning);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromParquets(const vector<string> &file_globs, bool binary_as_string,
                                                               bool file_row_number, bool filename,
                                                               bool hive_partitioning,
                                                               shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromParquets(file_globs, binary_as_string, file_row_number, filename, hive_partitioning);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromArrow(py::object &arrow_object,
                                                            shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromArrow(arrow_object);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::FromSubstrait(py::bytes &proto, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromSubstrait(proto);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::GetSubstrait(const string &query,
                                                               shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->GetSubstrait(query);
}

unique_ptr<DuckDBPyRelation> PyConnectionWrapper::GetSubstraitJSON(const string &query,
                                                                   shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->GetSubstraitJSON(query);
}

unordered_set<string> PyConnectionWrapper::GetTableNames(const string &query, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->GetTableNames(query);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::UnregisterPythonObject(const string &name,
                                                                           shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->UnregisterPythonObject(name);
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Begin(shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->Begin();
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Commit(shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->Commit();
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Rollback(shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->Rollback();
}

void PyConnectionWrapper::Close(shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	conn->Close();
}

shared_ptr<DuckDBPyConnection> PyConnectionWrapper::Cursor(shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->Cursor();
}

py::list PyConnectionWrapper::GetDescription(shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->GetDescription();
}

py::tuple PyConnectionWrapper::FetchOne(shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FetchOne();
}

py::list PyConnectionWrapper::FetchMany(idx_t size, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FetchMany(size);
}

py::list PyConnectionWrapper::FetchAll(shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FetchAll();
}

py::dict PyConnectionWrapper::FetchNumpy(shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FetchNumpy();
}

DataFrame PyConnectionWrapper::FetchDF(bool date_as_object, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FetchDF(date_as_object);
}

DataFrame PyConnectionWrapper::FetchDFChunk(const idx_t vectors_per_chunk, bool date_as_object,
                                            shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FetchDFChunk(vectors_per_chunk, date_as_object);
}

duckdb::pyarrow::Table PyConnectionWrapper::FetchArrow(idx_t chunk_size, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FetchArrow(chunk_size);
}

duckdb::pyarrow::RecordBatchReader PyConnectionWrapper::FetchRecordBatchReader(const idx_t chunk_size,
                                                                               shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FetchRecordBatchReader(chunk_size);
}

} // namespace duckdb
