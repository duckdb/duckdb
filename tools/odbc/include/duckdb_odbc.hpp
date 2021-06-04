#pragma once

// needs to be first because BOOL
#include "duckdb.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

extern "C" {
// handles
SQLRETURN SQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE *OutputHandlePtr);
SQLRETURN SQLFreeHandle(SQLSMALLINT HandleType, SQLHANDLE Handle);

// attributes
SQLRETURN SQLSetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength);
SQLRETURN SQLSetConnectAttr(SQLHDBC ConnectionHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                            SQLINTEGER StringLength);
SQLRETURN SQLSetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength);

// connections
SQLRETURN SQLDriverConnect(SQLHDBC ConnectionHandle, SQLHWND WindowHandle, SQLCHAR *InConnectionString,
                           SQLSMALLINT StringLength1, SQLCHAR *OutConnectionString, SQLSMALLINT BufferLength,
                           SQLSMALLINT *StringLength2Ptr, SQLUSMALLINT DriverCompletion);
SQLRETURN SQLGetInfo(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType, SQLPOINTER InfoValuePtr, SQLSMALLINT BufferLength,
                     SQLSMALLINT *StringLengthPtr);
SQLRETURN SQLEndTran(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT CompletionType);
SQLRETURN SQLDisconnect(SQLHDBC ConnectionHandle);

// statements
SQLRETURN SQLTables(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1, SQLCHAR *SchemaName,
                    SQLSMALLINT NameLength2, SQLCHAR *TableName, SQLSMALLINT NameLength3, SQLCHAR *TableType,
                    SQLSMALLINT NameLength4);
SQLRETURN SQLColumns(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1, SQLCHAR *SchemaName,
                     SQLSMALLINT NameLength2, SQLCHAR *TableName, SQLSMALLINT NameLength3, SQLCHAR *ColumnName,
                     SQLSMALLINT NameLength4);

SQLRETURN SQLPrepare(SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength);
SQLRETURN SQLExecDirect(SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength);
SQLRETURN SQLFreeStmt(SQLHSTMT StatementHandle, SQLUSMALLINT Option);
SQLRETURN SQLDescribeParam(SQLHSTMT StatementHandle, SQLUSMALLINT ParameterNumber, SQLSMALLINT *DataTypePtr,
                           SQLULEN *ParameterSizePtr, SQLSMALLINT *DecimalDigitsPtr, SQLSMALLINT *NullablePtr);
SQLRETURN SQLDescribeCol(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName,
                         SQLSMALLINT BufferLength, SQLSMALLINT *NameLengthPtr, SQLSMALLINT *DataTypePtr,
                         SQLULEN *ColumnSizePtr, SQLSMALLINT *DecimalDigitsPtr, SQLSMALLINT *NullablePtr);
SQLRETURN SQLColAttribute(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLUSMALLINT FieldIdentifier,
                          SQLPOINTER CharacterAttributePtr, SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr,
                          SQLLEN *NumericAttributePtr);
SQLRETURN SQLFetchScroll(SQLHSTMT StatementHandle, SQLSMALLINT FetchOrientation, SQLLEN FetchOffset);
SQLRETURN SQLRowCount(SQLHSTMT StatementHandle, SQLLEN *RowCountPtr);

// diagnostics
SQLRETURN SQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
                          SQLPOINTER DiagInfoPtr, SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr);
SQLRETURN SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLCHAR *SQLState,
                        SQLINTEGER *NativeErrorPtr, SQLCHAR *MessageText, SQLSMALLINT BufferLength,
                        SQLSMALLINT *TextLengthPtr);

} // extern "C"

namespace duckdb {
enum OdbcHandleType { ENV, DBC, STMT };
struct OdbcHandle {
	OdbcHandle(OdbcHandleType type_p) : type(type_p) {};
	OdbcHandleType type;
};

struct OdbcHandleEnv : public OdbcHandle {
	OdbcHandleEnv() : OdbcHandle(OdbcHandleType::ENV), db(make_unique<DuckDB>(nullptr)) {};
	unique_ptr<DuckDB> db;
};

struct OdbcHandleDbc : public OdbcHandle {
	OdbcHandleDbc(OdbcHandleEnv *env_p) : OdbcHandle(OdbcHandleType::DBC), env(env_p) {
		D_ASSERT(env_p);
		D_ASSERT(env_p->db);
	};
	OdbcHandleEnv *env;
	unique_ptr<Connection> conn;
};

struct OdbcBoundCol {
	OdbcBoundCol() : type(SQL_UNKNOWN_TYPE), ptr(nullptr), len(0), strlen_or_ind(nullptr) {};

	bool IsBound() {
		return ptr != nullptr;
	}

	SQLSMALLINT type;
	SQLPOINTER ptr;
	SQLLEN len;
	SQLLEN *strlen_or_ind;
};

struct OdbcHandleStmt : public OdbcHandle {
	OdbcHandleStmt(OdbcHandleDbc *dbc_p) : OdbcHandle(OdbcHandleType::STMT), dbc(dbc_p), rows_fetched_ptr(nullptr) {
		D_ASSERT(dbc_p);
		D_ASSERT(dbc_p->conn);
	};

	OdbcHandleDbc *dbc;
	unique_ptr<PreparedStatement> stmt;
	unique_ptr<QueryResult> res;
	unique_ptr<DataChunk> chunk;
	vector<Value> params;
	vector<OdbcBoundCol> bound_cols;
	bool open;
	row_t chunk_row;
	SQLULEN *rows_fetched_ptr;
};

struct OdbcUtils {
	static string ReadString(const SQLPOINTER ptr, const SQLSMALLINT len) {
		return len == SQL_NTS ? string((const char *)ptr) : string((const char *)ptr, (size_t)len);
	}

	static void WriteString(string &s, SQLCHAR *out_buf, SQLSMALLINT buf_len, SQLSMALLINT *out_len) {
		auto printf_len = snprintf((char *)out_buf, buf_len, "%s", s.c_str());
		if (out_len) {
			*out_len = printf_len;
		}
	}
};

template <class T>
SQLRETURN WithConnection(SQLHANDLE &ConnectionHandle, T &&lambda) {
	if (!ConnectionHandle) {
		return SQL_ERROR;
	}
	auto *hdl = (OdbcHandleDbc *)ConnectionHandle;
	if (hdl->type != OdbcHandleType::DBC) {
		return SQL_ERROR;
	}
	if (!hdl->conn) {
		return SQL_ERROR;
	}

	return lambda(hdl);
}

template <class T>
SQLRETURN WithStatement(SQLHANDLE &StatementHandle, T &&lambda) {
	if (!StatementHandle) {
		return SQL_ERROR;
	}
	auto *hdl = (OdbcHandleStmt *)StatementHandle;
	if (hdl->type != OdbcHandleType::STMT) {
		return SQL_ERROR;
	}
	if (!hdl->dbc || !hdl->dbc->conn) {
		return SQL_ERROR;
	}

	return lambda(hdl);
}

template <class T>
SQLRETURN WithStatementPrepared(SQLHANDLE &StatementHandle, T &&lambda) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) {
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
SQLRETURN WithStatementResult(SQLHANDLE &StatementHandle, T &&lambda) {
	return WithStatementPrepared(StatementHandle, [&](OdbcHandleStmt *stmt) {
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
