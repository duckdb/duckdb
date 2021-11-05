#ifndef DUCKDB_ODBC_HPP
#define DUCKDB_ODBC_HPP

// needs to be first because BOOL
#include "duckdb.hpp"

#include "duckdb/common/windows.hpp"

#include <sqltypes.h>
#include <sqlext.h>

#ifdef _WIN32
#include <Windows.h>
#endif

namespace duckdb {

class OdbcFetch;
class ParameterWrapper;

enum OdbcHandleType { ENV, DBC, STMT, DESC };
struct OdbcHandle {
	explicit OdbcHandle(OdbcHandleType type_p) : type(type_p) {};
	OdbcHandleType type;
};

struct OdbcHandleEnv : public OdbcHandle {
	OdbcHandleEnv() : OdbcHandle(OdbcHandleType::ENV), db(make_unique<DuckDB>(nullptr)) {};
	unique_ptr<DuckDB> db;
};

struct OdbcHandleStmt;

struct OdbcHandleDbc : public OdbcHandle {
	explicit OdbcHandleDbc(OdbcHandleEnv *env_p) : OdbcHandle(OdbcHandleType::DBC), env(env_p), autocommit(true) {
		D_ASSERT(env_p);
		D_ASSERT(env_p->db);
	};
	~OdbcHandleDbc();
	void EraseStmtRef(OdbcHandleStmt *stmt);
	SQLRETURN MaterializeResult();

	OdbcHandleEnv *env;
	unique_ptr<Connection> conn;
	bool autocommit;
	// reference to an open statement handled by this connection
	std::vector<OdbcHandleStmt *> vec_stmt_ref;
};

inline bool IsSQLVariableLengthType(SQLSMALLINT type) {
	if (type == SQL_CHAR || type == SQL_VARCHAR || type == SQL_WVARCHAR || type == SQL_BINARY) {
		return true;
	}
	return false;
}

struct OdbcBoundCol {
	OdbcBoundCol() : type(SQL_UNKNOWN_TYPE), ptr(nullptr), len(0), strlen_or_ind(nullptr) {};

	bool IsBound() {
		return ptr != nullptr;
	}

	bool IsVarcharBound() {
		if (IsSQLVariableLengthType(type)) {
			return strlen_or_ind != nullptr;
		}
		return false;
	}

	SQLSMALLINT type;
	SQLPOINTER ptr;
	SQLLEN len;
	SQLLEN *strlen_or_ind;
};

struct OdbcHandleDesc;

struct OdbcHandleStmt : public OdbcHandle {
	explicit OdbcHandleStmt(OdbcHandleDbc *dbc_p);
	~OdbcHandleStmt();
	void Close();
	SQLRETURN MaterializeResult();

	OdbcHandleDbc *dbc;
	unique_ptr<PreparedStatement> stmt;
	unique_ptr<QueryResult> res;
	unique_ptr<ParameterWrapper> param_wrapper;
	vector<OdbcBoundCol> bound_cols;
	bool open;
	SQLULEN *rows_fetched_ptr;
	// appending all statement error messages into it
	vector<std::string> error_messages;
	// fetcher
	unique_ptr<OdbcFetch> odbc_fetcher;

	unique_ptr<OdbcHandleDesc> apd;
	unique_ptr<OdbcHandleDesc> ipd;
	unique_ptr<OdbcHandleDesc> ard;
	unique_ptr<OdbcHandleDesc> ird;
};

struct OdbcHandleDesc : public OdbcHandle {
	//! https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/descriptors?view=sql-server-ver15
	// TODO requires full implmentation
	explicit OdbcHandleDesc() : OdbcHandle(OdbcHandleType::DESC) {};
	~OdbcHandleDesc() {};
};

struct OdbcUtils {
	static string ReadString(const SQLPOINTER ptr, const SQLSMALLINT len) {
		return len == SQL_NTS ? string((const char *)ptr) : string((const char *)ptr, (size_t)len);
	}

	static void WriteString(const string &s, SQLCHAR *out_buf, SQLSMALLINT buf_len, SQLSMALLINT *out_len) {
		if (out_buf) {
			snprintf((char *)out_buf, buf_len, "%s", s.c_str());
		}
		if (out_len) {
			*out_len = s.size();
		}
	}
};

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

	return lambda(hdl);
}

template <class T>
SQLRETURN WithStatement(SQLHANDLE &statement_handle, T &&lambda) {
	if (!statement_handle) {
		return SQL_ERROR;
	}
	auto *hdl = (OdbcHandleStmt *)statement_handle;
	if (hdl->type != OdbcHandleType::STMT) {
		return SQL_ERROR;
	}
	if (!hdl->dbc || !hdl->dbc->conn) {
		return SQL_ERROR;
	}
	return lambda(hdl);
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
		return lambda(stmt);
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
		return lambda(stmt);
	});
}

} // namespace duckdb

#endif // DUCKDB_ODBC_HPP
