#include "../common.h"

#include <iostream>

using namespace odbc_test;

/**
	 * Execute a query that generates a result set
 */
static void BasicCursorCommitTest(HSTMT &hstmt) {
	EXECUTE_AND_CHECK("SQLSetStmtAttr", SQLSetStmtAttr, hstmt, SQL_ATTR_CURSOR_TYPE, ConvertToSQLPOINTER(SQL_CURSOR_STATIC), SQL_IS_UINTEGER);

	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT g FROM generate_series(1,3) g(g)"),
	                  SQL_NTS);

	char buf[1024];
	SQLLEN buf_len;
	EXECUTE_AND_CHECK("SQLBindCol", SQLBindCol, hstmt, 1, SQL_C_CHAR, &buf, sizeof(buf), &buf_len);

	// Commit. This implicitly closes the cursor in the server.
	EXECUTE_AND_CHECK("SQLEndTran", SQLEndTran, SQL_HANDLE_DBC, dbc, SQL_COMMIT);

	for (char i = 1; i < 4; i++) {
		EXECUTE_AND_CHECK("SQLFetchScroll", SQLFetchScroll, hstmt, SQL_FETCH_NEXT, 0);
		REQUIRE(buf_len == 1);
		REQUIRE(STR_EQUAL(buf, ConvertToCString(i)));
	}

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

TEST_CASE("Test Cursor Commit", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLSetConnectAttr", SQLSetConnectAttr, dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, SQL_IS_INTEGER);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	BasicCursorCommitTest(hstmt);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
