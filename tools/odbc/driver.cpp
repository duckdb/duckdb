#include "duckdb_odbc.hpp"
using namespace duckdb;

SQLRETURN SQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE *OutputHandlePtr) {
	switch (HandleType) {
	case SQL_HANDLE_DBC: {
		D_ASSERT(InputHandle);
		auto *env = (OdbcHandleEnv *)InputHandle;
		D_ASSERT(env->type == OdbcHandleType::ENV);
		*OutputHandlePtr = new OdbcHandleDbc(env);
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC:
		throw std::runtime_error("SQL_HANDLE_DESC");
	case SQL_HANDLE_ENV:
		*OutputHandlePtr = new OdbcHandleEnv();
		return SQL_SUCCESS;
	case SQL_HANDLE_STMT: {
		D_ASSERT(InputHandle);
		auto *dbc = (OdbcHandleDbc *)InputHandle;
		D_ASSERT(dbc->type == OdbcHandleType::DBC);
		*OutputHandlePtr = new OdbcHandleStmt(dbc);
		return SQL_SUCCESS;
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLFreeHandle(SQLSMALLINT HandleType, SQLHANDLE Handle) {
	if (!Handle) {
		return SQL_ERROR;
	}

	switch (HandleType) {
	case SQL_HANDLE_DBC: {
		auto *hdl = (OdbcHandleDbc *)Handle;
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC:
		throw std::runtime_error("SQL_HANDLE_DESC");
	case SQL_HANDLE_ENV: {
		auto *hdl = (OdbcHandleEnv *)Handle;
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_STMT: {
		auto *hdl = (OdbcHandleStmt *)Handle;
		delete hdl;
		return SQL_SUCCESS;
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLSetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength) {
	if (!EnvironmentHandle) {
		return SQL_ERROR;
	}
	auto *env = (OdbcHandleEnv *)EnvironmentHandle;
	if (env->type != OdbcHandleType::ENV) {
		return SQL_ERROR;
	}
	switch (Attribute) {
	case SQL_ATTR_ODBC_VERSION: {
		auto version = (SQLINTEGER)(uintptr_t)ValuePtr;
		// TODO actually do something with this?
		return SQL_SUCCESS;
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLDriverConnect(SQLHDBC ConnectionHandle, SQLHWND WindowHandle, SQLCHAR *InConnectionString,
                           SQLSMALLINT StringLength1, SQLCHAR *OutConnectionString, SQLSMALLINT BufferLength,
                           SQLSMALLINT *StringLength2Ptr, SQLUSMALLINT DriverCompletion) {
	// TODO actually interpret Database in InConnectionString
	if (!ConnectionHandle) {
		return SQL_ERROR;
	}
	auto *dbc = (OdbcHandleDbc *)ConnectionHandle;
	if (dbc->type != OdbcHandleType::DBC) {
		return SQL_ERROR;
	}
	if (!dbc->conn) {
		dbc->conn = make_unique<Connection>(*dbc->env->db);
		dbc->conn->SetAutoCommit(dbc->autocommit);
	}
	return SQL_SUCCESS;
}

SQLRETURN SQLConnect(SQLHDBC ConnectionHandle, SQLCHAR *ServerName, SQLSMALLINT NameLength1, SQLCHAR *UserName,
                     SQLSMALLINT NameLength2, SQLCHAR *Authentication, SQLSMALLINT NameLength3) {
	// TODO this is duplicated from above, but ServerName is just the DSN
	if (!ConnectionHandle) {
		return SQL_ERROR;
	}
	auto *dbc = (OdbcHandleDbc *)ConnectionHandle;
	if (dbc->type != OdbcHandleType::DBC) {
		return SQL_ERROR;
	}
	if (!dbc->conn) {
		dbc->conn = make_unique<Connection>(*dbc->env->db);
		dbc->conn->SetAutoCommit(dbc->autocommit);
	}
	return SQL_SUCCESS;
}

SQLRETURN SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLCHAR *SQLState,
                        SQLINTEGER *NativeErrorPtr, SQLCHAR *MessageText, SQLSMALLINT BufferLength,
                        SQLSMALLINT *TextLengthPtr) {

	if (!Handle) {
		return SQL_ERROR;
	}
	if (RecNumber != 1) { // TODO is it just trying increasing rec numbers?
		return SQL_ERROR;
	}

	if (SQLState) {
		*SQLState = 0;
	}

	if (NativeErrorPtr) {
		*NativeErrorPtr = 0; // we don't have error codes
	}

	auto *hdl = (OdbcHandle *)Handle;

	switch (HandleType) {
	case SQL_HANDLE_DBC: {
		// TODO return connection errors here
		return SQL_ERROR;
	}
	case SQL_HANDLE_DESC:
		throw std::runtime_error("SQL_HANDLE_DESC");
	case SQL_HANDLE_ENV: {
		// dont think we can have errors here
		return SQL_ERROR;
	}
	case SQL_HANDLE_STMT: {
		if (hdl->type != OdbcHandleType::STMT) {
			return SQL_ERROR;
		}
		auto *stmt = (OdbcHandleStmt *)hdl;
		if (stmt->stmt && !stmt->stmt->success) {
			OdbcUtils::WriteString(stmt->stmt->error, MessageText, BufferLength, TextLengthPtr);
			return SQL_SUCCESS;
		}
		if (stmt->res && !stmt->res->success) {
			OdbcUtils::WriteString(stmt->res->error, MessageText, BufferLength, TextLengthPtr);
			return SQL_SUCCESS;
		}
		return SQL_ERROR;
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
                          SQLPOINTER DiagInfoPtr, SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr) {
	throw std::runtime_error("SQLGetDiagField");
}