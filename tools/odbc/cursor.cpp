#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "handle_functions.hpp"

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT statement_handle) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	if (ConvertHSTMT(statement_handle, hstmt) != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	return duckdb::CloseStmt(hstmt);
}

SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT statement_handle, SQLCHAR *cursor_name, SQLSMALLINT name_length) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	if (ConvertHSTMT(statement_handle, hstmt) != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	hstmt->error_messages.emplace_back("SQLSetCursorName is not supported.");
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT statement_handle, SQLCHAR *cursor_name, SQLSMALLINT name_length) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	if (ConvertHSTMT(statement_handle, hstmt) != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	hstmt->error_messages.emplace_back("SQLGetCursorName is not supported.");
	return SQL_ERROR;
}
