#include "../common.h"

using namespace odbc_test;

TEST_CASE("Alter", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database
	CONNECT_TO_DATABASE(ret, env, dbc);

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLAllocHandle (HSTMT)");

	// Create a table to test with
	ret = SQLExecDirect(hstmt, (SQLCHAR *)"CREATE TABLE testtbl(t varchar(40))", SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLExecDirect (CREATE TABLE)");

	// A simple query against the table, fetch column info
	ret = SQLExecDirect(hstmt, (SQLCHAR *)"SELECT * FROM testtbl", SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLExecDirect (SELECT)");

	// Get column metadata
	SQLSMALLINT num_cols;
	ret = SQLNumResultCols(hstmt, &num_cols);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLNumResultCols");
	REQUIRE(num_cols == 1);

	// Retrieve metadata from the column
	METADATA_CHECK(ret, hstmt, num_cols, "t", sizeof('t'), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLDescribeCol");

	// Alter the table
	ret = SQLExecDirect(hstmt, (SQLCHAR *)"ALTER TABLE testtbl ALTER t SET DATA TYPE varchar", SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLExecDirect (ALTER TABLE)");

	// Rerun the query to check if the metadata was updated
	ret = SQLExecDirect(hstmt, (SQLCHAR *)"SELECT * FROM testtbl", SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLExecDirect (SELECT)");

	// Get column metadata
	SQLSMALLINT num_cols2;
	ret = SQLNumResultCols(hstmt, &num_cols2);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLNumResultCols");
	REQUIRE(num_cols2 == 1);

	// Retrieve metadata from the column
	METADATA_CHECK(ret, hstmt, num_cols2, "t", sizeof('t'), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLDescribeCol");

	ret = SQLFreeStmt(hstmt, SQL_CLOSE);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFreeStmt (SQL_CLOSE)");

	DISCONNECT_FROM_DATABASE(ret, dbc, env);
}
