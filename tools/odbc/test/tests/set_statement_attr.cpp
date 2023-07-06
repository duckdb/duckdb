#include "../common.h"

#include <iostream>

using namespace odbc_test;

TEST_CASE("Test SQL_ATTR_ROW_BIND_TYPE attribute in SQLSetStmtAttr", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Set the statement attribute SQL_ATTR_ROW_BIND_TYPE
	uint64_t row_len = 256;
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_BIND_TYPE)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_BIND_TYPE,
	                  (SQLPOINTER)row_len, SQL_IS_INTEGER);

	// Check the statement attribute SQL_ATTR_ROW_BIND_TYPE
	SQLULEN buf;
	EXECUTE_AND_CHECK("SQLGetStmtAttr (SQL_ATTR_ROW_BIND_TYPE)", SQLGetStmtAttr, hstmt, SQL_ATTR_ROW_BIND_TYPE,
	                  &buf, sizeof(buf), nullptr);
	REQUIRE(row_len == buf);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
