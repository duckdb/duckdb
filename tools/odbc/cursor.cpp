#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT statement_handle) {
	return duckdb::WithStatement(statement_handle,
	                             [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN { return duckdb::CloseStmt(stmt); });
}

SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT statement_handle, SQLCHAR *cursor_name, SQLSMALLINT name_length) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN {
		stmt->error_messages.emplace_back("SQLSetCursorName does not supported.");
		return SQL_ERROR;
	});
}

SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT statement_handle, SQLCHAR *cursor_name, SQLSMALLINT name_length) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN {
		stmt->error_messages.emplace_back("SQLGetCursorName does not supported.");
		return SQL_ERROR;
	});
}
