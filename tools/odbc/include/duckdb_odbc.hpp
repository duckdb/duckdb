#ifndef DUCKDB_ODBC_HPP
#define DUCKDB_ODBC_HPP

// needs to be first because BOOL
#include "duckdb.hpp"

#include "duckdb/common/windows.hpp"
#include "descriptor.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_exception.hpp"
#include "odbc_utils.hpp"

#include <sqltypes.h>
#include <sqlext.h>
#include <vector>

#ifdef _WIN32
#include <Windows.h>
#endif

namespace duckdb {

class OdbcFetch;
class ParameterDescriptor;
class RowDescriptor;
class OdbcDiagnostic;

enum OdbcHandleType { ENV, DBC, STMT, DESC };
std::string OdbcHandleTypeToString(OdbcHandleType type);

struct OdbcHandle {
	explicit OdbcHandle(OdbcHandleType type_p);
	OdbcHandle(const OdbcHandle &other);
	OdbcHandle &operator=(const OdbcHandle &other);

	OdbcHandleType type;
	// appending all error messages into it
	std::vector<std::string> error_messages;

	unique_ptr<OdbcDiagnostic> odbc_diagnostic;
};

struct OdbcHandleEnv : public OdbcHandle {
	OdbcHandleEnv() : OdbcHandle(OdbcHandleType::ENV), db(make_unique<DuckDB>(nullptr)) {};
	unique_ptr<DuckDB> db;
};

struct OdbcHandleStmt;
struct OdbcHandleDesc;

struct OdbcHandleDbc : public OdbcHandle {
public:
	explicit OdbcHandleDbc(OdbcHandleEnv *env_p)
	    : OdbcHandle(OdbcHandleType::DBC), env(env_p), autocommit(true), sql_attr_access_mode(SQL_MODE_READ_WRITE) {
		D_ASSERT(env_p);
		D_ASSERT(env_p->db);
	};
	~OdbcHandleDbc();
	void EraseStmtRef(OdbcHandleStmt *stmt);
	SQLRETURN MaterializeResult();
	void ResetStmtDescriptors(OdbcHandleDesc *old_desc);

	void SetDatabaseName(const string &db_name);
	std::string GetDatabaseName();
	std::string GetDataSourceName();

public:
	OdbcHandleEnv *env;
	unique_ptr<Connection> conn;
	bool autocommit;
	SQLUINTEGER sql_attr_metadata_id;
	SQLUINTEGER sql_attr_access_mode;
	// this is the database name, see: SQLSetConnectAttr
	std::string sql_attr_current_catalog;
	// this is DSN get in string connection, see: SQLConnect
	// Ex: "DSN=DuckDB"
	std::string dsn;
	// reference to an open statement handled by this connection
	std::vector<OdbcHandleStmt *> vec_stmt_ref;
};

struct OdbcBoundCol {
	OdbcBoundCol() : type(SQL_UNKNOWN_TYPE), ptr(nullptr), len(0), strlen_or_ind(nullptr) {};

	bool IsBound() {
		return ptr != nullptr;
	}

	bool IsVarcharBound() {
		if (OdbcUtils::IsCharType(type)) {
			return strlen_or_ind != nullptr;
		}
		return false;
	}

	SQLSMALLINT type;
	SQLPOINTER ptr;
	SQLLEN len;
	SQLLEN *strlen_or_ind;
};

struct OdbcHandleStmt : public OdbcHandle {
public:
	explicit OdbcHandleStmt(OdbcHandleDbc *dbc_p);
	~OdbcHandleStmt();
	void Close();
	SQLRETURN MaterializeResult();
	void SetARD(OdbcHandleDesc *new_ard);
	void SetAPD(OdbcHandleDesc *new_apd);
	bool IsPrepared() {
		return stmt != nullptr;
	}
	void FillIRD();

public:
	OdbcHandleDbc *dbc;
	unique_ptr<PreparedStatement> stmt;
	unique_ptr<QueryResult> res;
	vector<OdbcBoundCol> bound_cols;
	bool open;
	SQLULEN retrieve_data = SQL_RD_ON;
	SQLULEN *rows_fetched_ptr;

	// fetcher
	unique_ptr<OdbcFetch> odbc_fetcher;

	unique_ptr<ParameterDescriptor> param_desc;

	unique_ptr<RowDescriptor> row_desc;
};

struct OdbcHandleDesc : public OdbcHandle {
	//! https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/descriptors?view=sql-server-ver15
	// TODO requires full implmentation
public:
	explicit OdbcHandleDesc(OdbcHandleDbc *dbc_ptr = nullptr, OdbcHandleStmt *stmt_ptr = nullptr,
	                        bool explicit_desc = false)
	    : OdbcHandle(OdbcHandleType::DESC), dbc(dbc_ptr), stmt(stmt_ptr) {
		header.sql_desc_alloc_type = SQL_DESC_ALLOC_AUTO;
		if (explicit_desc) {
			header.sql_desc_alloc_type = SQL_DESC_ALLOC_USER;
		}
	}
	OdbcHandleDesc(const OdbcHandleDesc &other);
	~OdbcHandleDesc() {
	}
	OdbcHandleDesc &operator=(const OdbcHandleDesc &other);
	void CopyOnlyOdbcFields(const OdbcHandleDesc &other);
	void CopyFieldByField(const OdbcHandleDesc &other);

	DescRecord *GetDescRecord(idx_t param_idx);
	SQLRETURN SetDescField(SQLSMALLINT rec_number, SQLSMALLINT field_identifier, SQLPOINTER value_ptr,
	                       SQLINTEGER buffer_length);
	void Clear();
	void Reset();
	void Copy(OdbcHandleDesc &other);

	// verify Implementation Descriptor (ID)
	bool IsID();
	// verify Application Descriptor (AD)
	bool IsAD();
	bool IsIRD();
	bool IsIPD();

	void AddMoreRecords(SQLSMALLINT new_size);

public:
	DescHeader header;
	std::vector<DescRecord> records;
	OdbcHandleDbc *dbc;
	OdbcHandleStmt *stmt;
};

template <class T>
SQLRETURN WithHandle(SQLHANDLE &handle, T &&lambda) {
	if (!handle) {
		return SQL_INVALID_HANDLE;
	}
	auto *hdl = (OdbcHandle *)handle;
	if (!hdl->odbc_diagnostic) {
		return SQL_ERROR;
	}

	return lambda(hdl);
}

template <class T>
SQLRETURN WithEnvironment(SQLHANDLE &enviroment_handle, T &&lambda) {
	if (!enviroment_handle) {
		return SQL_ERROR;
	}
	auto *env = (OdbcHandleEnv *)enviroment_handle;
	if (env->type != OdbcHandleType::ENV) {
		return SQL_ERROR;
	}
	if (!env->db) {
		return SQL_ERROR;
	}

	return lambda(env);
}

template <class T>
SQLRETURN WithConnection(SQLHANDLE &connection_handle, T &&lambda) {
	if (!connection_handle) {
		return SQL_ERROR;
	}
	auto *hdl = (OdbcHandleDbc *)connection_handle;
	if (hdl->type != OdbcHandleType::DBC) {
		return SQL_ERROR;
	}
	if (!hdl->conn) {
		return SQL_ERROR;
	}

	// ODBC requires to clean up the diagnostic for every ODBC function call
	hdl->odbc_diagnostic->Clean();

	try {
		return lambda(hdl);
	} catch (OdbcException &ex) {
		auto diag_record = ex.GetDiagRecord();
		auto component = ex.GetComponent();
		auto data_source = hdl->GetDataSourceName();
		hdl->odbc_diagnostic->FormatDiagnosticMessage(diag_record, data_source, component);
		hdl->odbc_diagnostic->AddDiagRecord(diag_record);
		return ex.GetSqlReturn();
	}
}

template <class T>
SQLRETURN WithStatement(SQLHANDLE &statement_handle, T &&lambda) {
	if (!statement_handle) {
		return SQL_ERROR;
	}
	auto *hdl_stmt = (OdbcHandleStmt *)statement_handle;
	if (hdl_stmt->type != OdbcHandleType::STMT) {
		return SQL_ERROR;
	}
	if (!hdl_stmt->dbc || !hdl_stmt->dbc->conn) {
		return SQL_ERROR;
	}

	// ODBC requires to clean up the diagnostic for every ODBC function call
	hdl_stmt->odbc_diagnostic->Clean();

	try {
		return lambda(hdl_stmt);
	} catch (OdbcException &ex) {
		auto diag_record = ex.GetDiagRecord();
		auto component = ex.GetComponent();
		auto data_source = hdl_stmt->dbc->GetDataSourceName();
		hdl_stmt->odbc_diagnostic->FormatDiagnosticMessage(diag_record, data_source, component);
		hdl_stmt->odbc_diagnostic->AddDiagRecord(diag_record);
		return ex.GetSqlReturn();
	}
}

template <class T>
SQLRETURN WithStatementPrepared(SQLHANDLE &statement_handle, T &&lambda) {
	return WithStatement(statement_handle, [&](OdbcHandleStmt *stmt) -> SQLRETURN {
		if (!stmt->stmt) {
			return SQL_ERROR;
		}
		if (!stmt->stmt->success) {
			return SQL_ERROR;
		}
		try {
			return lambda(stmt);
		} catch (OdbcException &ex) {
			throw ex;
		}
	});
}

template <class T>
SQLRETURN WithStatementResult(SQLHANDLE &statement_handle, T &&lambda) {
	return WithStatementPrepared(statement_handle, [&](OdbcHandleStmt *stmt) -> SQLRETURN {
		if (!stmt->res) {
			return SQL_ERROR;
		}
		if (!stmt->res->success) {
			return SQL_ERROR;
		}
		try {
			return lambda(stmt);
		} catch (OdbcException &ex) {
			throw ex;
		}
	});
}

template <class T>
SQLRETURN WithDescriptor(SQLHANDLE &descriptor_handle, T &&lambda) {
	if (!descriptor_handle) {
		return SQL_ERROR;
	}
	auto *hdl = (OdbcHandleDesc *)descriptor_handle;
	if (hdl->type != OdbcHandleType::DESC) {
		return SQL_ERROR;
	}
	return lambda(hdl);
}

} // namespace duckdb

#endif // DUCKDB_ODBC_HPP
