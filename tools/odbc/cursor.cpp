#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "handle_functions.hpp"

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT statement_handle) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::CloseStmt(hstmt);
}

SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT statement_handle, SQLCHAR *cursor_name, SQLSMALLINT name_length) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	hstmt->error_messages.emplace_back("SQLSetCursorName is not supported.");
	return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLSetCursorName", hstmt->error_messages.back(), duckdb::SQLStateType::DRIVER_NOT_SUPPORT_FUNCTION, hstmt->dbc->GetDataSourceName());
}

SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT statement_handle, SQLCHAR *cursor_name, SQLSMALLINT name_length) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	hstmt->error_messages.emplace_back("SQLGetCursorName is not supported.");
	return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLSetCursorName", hstmt->error_messages.back(), duckdb::SQLStateType::DRIVER_NOT_SUPPORT_FUNCTION, hstmt->dbc->GetDataSourceName());
}
