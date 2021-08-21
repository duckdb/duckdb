#ifndef DUCKDB_ODBC_HPP
#define DUCKDB_ODBC_HPP

// needs to be first because BOOL
#include "duckdb.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

extern "C" {
// handles
SQLRETURN SQLAllocHandle(SQLSMALLINT handle_type, SQLHANDLE input_handle, SQLHANDLE *output_handle_ptr);
SQLRETURN SQLFreeHandle(SQLSMALLINT handle_type, SQLHANDLE handle);

// attributes
SQLRETURN SQLGetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                            SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr);
SQLRETURN SQLSetEnvAttr(SQLHENV environment_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                        SQLINTEGER string_length);
SQLRETURN SQLSetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                            SQLINTEGER string_length);
SQLRETURN SQLSetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                         SQLINTEGER string_length);

// connections
SQLRETURN SQLDriverConnect(SQLHDBC connection_handle, SQLHWND window_handle, SQLCHAR *in_connection_string,
                           SQLSMALLINT string_length1, SQLCHAR *out_connection_string, SQLSMALLINT buffer_length,
                           SQLSMALLINT *string_length2_ptr, SQLUSMALLINT driver_completion);
SQLRETURN SQLConnect(SQLHDBC connection_handle, SQLCHAR *server_name, SQLSMALLINT name_length1, SQLCHAR *user_name,
                     SQLSMALLINT name_length2, SQLCHAR *authentication, SQLSMALLINT name_length3);

SQLRETURN SQLGetInfo(SQLHDBC connection_handle, SQLUSMALLINT info_type, SQLPOINTER info_value_ptr,
                     SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr);
SQLRETURN SQLEndTran(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT completion_type);
SQLRETURN SQLDisconnect(SQLHDBC connection_handle);

// statements
SQLRETURN SQLTables(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1, SQLCHAR *schema_name,
                    SQLSMALLINT name_length2, SQLCHAR *table_name, SQLSMALLINT name_length3, SQLCHAR *table_type,
                    SQLSMALLINT name_length4);
SQLRETURN SQLColumns(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1, SQLCHAR *schema_name,
                     SQLSMALLINT name_length2, SQLCHAR *table_name, SQLSMALLINT name_length3, SQLCHAR *column_name,
                     SQLSMALLINT name_length4);

SQLRETURN SQLPrepare(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length);
SQLRETURN SQLExecDirect(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length);
SQLRETURN SQLFreeStmt(SQLHSTMT statement_handle, SQLUSMALLINT option);
SQLRETURN SQLDescribeParam(SQLHSTMT statement_handle, SQLUSMALLINT parameter_number, SQLSMALLINT *data_type_ptr,
                           SQLULEN *parameter_size_ptr, SQLSMALLINT *decimal_digits_ptr, SQLSMALLINT *nullable_ptr);
SQLRETURN SQLDescribeCol(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLCHAR *column_name,
                         SQLSMALLINT buffer_length, SQLSMALLINT *name_length_ptr, SQLSMALLINT *data_type_ptr,
                         SQLULEN *column_size_ptr, SQLSMALLINT *decimal_digits_ptr, SQLSMALLINT *nullable_ptr);
SQLRETURN SQLColAttribute(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLUSMALLINT field_identifier,
                          SQLPOINTER character_attribute_ptr, SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr,
                          SQLLEN *numeric_attribute_ptr);
SQLRETURN SQLFetchScroll(SQLHSTMT statement_handle, SQLSMALLINT fetch_orientation, SQLLEN fetch_offset);
SQLRETURN SQLRowCount(SQLHSTMT statement_handle, SQLLEN *row_count_ptr);

SQLRETURN SQLNumResultCols(SQLHSTMT statement_handle, SQLSMALLINT *column_count_ptr);

SQLRETURN SQLBindCol(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLSMALLINT target_type,
                     SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr);

SQLRETURN SQLPutData(SQLHSTMT statement_handle, SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr);

SQLRETURN SQLCancel(SQLHSTMT statement_handle);

SQLRETURN SQLNumParams(SQLHSTMT statement_handle, SQLSMALLINT *parameter_count_ptr);

SQLRETURN SQLParamData(SQLHSTMT statement_handle, SQLPOINTER *value_ptr_ptr);

SQLRETURN SQLMoreResults(SQLHSTMT statement_handle);

// diagnostics
SQLRETURN SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number,
                          SQLSMALLINT diag_identifier, SQLPOINTER diag_info_ptr, SQLSMALLINT buffer_length,
                          SQLSMALLINT *string_length_ptr);
SQLRETURN SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number, SQLCHAR *sql_state,
                        SQLINTEGER *native_error_ptr, SQLCHAR *message_text, SQLSMALLINT buffer_length,
                        SQLSMALLINT *text_length_ptr);

// api info
SQLRETURN SQLGetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr);
SQLRETURN SQLGetTypeInfo(SQLHSTMT statement_handle, SQLSMALLINT data_type);

} // extern "C"

namespace duckdb {

class OdbcFetch;
class ParameterWrapper;

enum OdbcHandleType { ENV, DBC, STMT };
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
	OdbcHandleEnv *env;
	unique_ptr<Connection> conn;
	bool autocommit;
	// reference to an open statement handled by this connection
	OdbcHandleStmt *stmt_handle;
};

inline bool IsSQLVarcharType(SQLSMALLINT type) {
	if (type == SQL_CHAR || type == SQL_VARCHAR || type == SQL_WVARCHAR) {
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
		if (IsSQLVarcharType(type)) {
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
	explicit OdbcHandleStmt(OdbcHandleDbc *dbc_p);
	~OdbcHandleStmt();
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
