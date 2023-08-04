#ifndef DUCKDB_ODBC_HPP
#define DUCKDB_ODBC_HPP

// needs to be first because BOOL
#include "duckdb.hpp"

#include "duckdb/common/windows.hpp"
#include "descriptor.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_utils.hpp"

#include <sqltypes.h>
#include <sqlext.h>
#include "duckdb/common/vector.hpp"

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
	vector<std::string> error_messages;

	duckdb::unique_ptr<OdbcDiagnostic> odbc_diagnostic;
};

struct OdbcHandleEnv : public OdbcHandle {
	OdbcHandleEnv() : OdbcHandle(OdbcHandleType::ENV), db(make_shared<DuckDB>(nullptr)) {};

	shared_ptr<DuckDB> db;
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
	duckdb::unique_ptr<Connection> conn;
	bool autocommit;
	SQLUINTEGER sql_attr_metadata_id;
	SQLUINTEGER sql_attr_access_mode;
	// this is the database name, see: SQLSetConnectAttr
	std::string sql_attr_current_catalog;
	// this is DSN get in string connection, see: SQLConnect
	// Ex: "DSN=DuckDB"
	std::string dsn;
	// reference to an open statement handled by this connection
	vector<OdbcHandleStmt *> vec_stmt_ref;
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
	duckdb::unique_ptr<PreparedStatement> stmt;
	duckdb::unique_ptr<QueryResult> res;
	vector<OdbcBoundCol> bound_cols;
	bool open;
	SQLULEN retrieve_data = SQL_RD_ON;
	SQLULEN *rows_fetched_ptr;

	// fetcher
	duckdb::unique_ptr<OdbcFetch> odbc_fetcher;

	duckdb::unique_ptr<ParameterDescriptor> param_desc;

	duckdb::unique_ptr<RowDescriptor> row_desc;
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
	vector<DescRecord> records;
	OdbcHandleDbc *dbc;
	OdbcHandleStmt *stmt;
};

} // namespace duckdb

#endif // DUCKDB_ODBC_HPP
