#include "../common.h"

using namespace odbc_test;

/**
 * Execute a query that generates a result set
 */
static void SimpleCursorCommitTest(SQLHANDLE dbc) {
	HSTMT hstmt = SQL_NULL_HSTMT;

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	EXECUTE_AND_CHECK("SQLSetStmtAttr", SQLSetStmtAttr, hstmt, SQL_ATTR_CURSOR_TYPE,
	                  ConvertToSQLPOINTER(SQL_CURSOR_STATIC), SQL_IS_UINTEGER);

	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT g FROM generate_series(1,3) g(g)"), SQL_NTS);

	char buf[1024];
	SQLLEN buf_len;
	EXECUTE_AND_CHECK("SQLBindCol", SQLBindCol, hstmt, 1, SQL_C_CHAR, &buf, sizeof(buf), &buf_len);

	// Commit. This implicitly closes the cursor in the server.
	EXECUTE_AND_CHECK("SQLEndTran", SQLEndTran, SQL_HANDLE_DBC, dbc, SQL_COMMIT);

	for (char i = 1; i < 4; i++) {
		EXECUTE_AND_CHECK("SQLFetchScroll", SQLFetchScroll, hstmt, SQL_FETCH_NEXT, 0);
		REQUIRE(buf_len == 1);
		REQUIRE(STR_EQUAL(buf, std::to_string(i).c_str()));
	}

	SQLRETURN ret = SQLFetchScroll(hstmt, SQL_FETCH_NEXT, 0);
	REQUIRE(ret == SQL_NO_DATA);

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
}

static void PreparedCursorCommitTest(SQLHANDLE dbc) {
	HSTMT hstmt = SQL_NULL_HSTMT;

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Try to commit without an open query
	EXECUTE_AND_CHECK("SQLEndTran", SQLEndTran, SQL_HANDLE_DBC, dbc, SQL_COMMIT);

	// Prepare a statement
	EXECUTE_AND_CHECK("SQLPrepare", SQLPrepare, hstmt, ConvertToSQLCHAR("SELECT ?::BOOL"), SQL_NTS);

	// Commit with a prepared statement
	EXECUTE_AND_CHECK("SQLEndTran", SQLEndTran, SQL_HANDLE_DBC, dbc, SQL_COMMIT);

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
}

static void MultipleHSTMTTest(SQLHANDLE dbc) {
	HSTMT hstmt1 = SQL_NULL_HSTMT;
	HSTMT hstmt2 = SQL_NULL_HSTMT;

	// Allocate a statement handles
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt1);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt2);

	// Execute queries on both statement handles
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt1,
	                  ConvertToSQLCHAR("SELECT g FROM generate_series(1,3) g(g)"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt2,
	                  ConvertToSQLCHAR("SELECT g FROM generate_series(1,3) g(g)"), SQL_NTS);

	// Free first statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt1, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt1);

	// Commit test after the first handle is released
	EXECUTE_AND_CHECK("SQLEndTran", SQLEndTran, SQL_HANDLE_DBC, dbc, SQL_COMMIT);

	SQLINTEGER buf;
	SQLLEN buf_len;

	EXECUTE_AND_CHECK("SQLBindCol", SQLBindCol, hstmt2, 1, SQL_C_SLONG, &buf, sizeof(buf), &buf_len);

	// Fetch from the second statement handle
	for (int i = 1; i < 4; i++) {
		EXECUTE_AND_CHECK("SQLFetchScroll", SQLFetchScroll, hstmt2, SQL_FETCH_NEXT, 0);
		REQUIRE(buf_len == sizeof(buf));
		REQUIRE(buf == i);
	}

	SQLRETURN ret = SQLFetchScroll(hstmt2, SQL_FETCH_NEXT, 0);
	REQUIRE(ret == SQL_NO_DATA);

	// Free second statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt2, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt2);
}

// These tests are related to cursor commit behavior.
// The cursor represents the result set of a query, and is closed when the transaction is committed.
TEST_CASE("Test setting cursor attributes, and closing the cursor", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLSetConnectAttr", SQLSetConnectAttr, dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF,
	                  SQL_IS_INTEGER);

	SimpleCursorCommitTest(dbc);
	PreparedCursorCommitTest(dbc);
	MultipleHSTMTTest(dbc);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
