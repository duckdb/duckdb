#include "../common.h"

using namespace odbc_test;

TEST_CASE("Alter", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database
	CONNECT_TO_DATABASE(ret, env, dbc);

	ExecuteCmdAndCheckODBC("SQLAllocHandle (HSTMT)", SQL_HANDLE_STMT, hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc,
	                       &hstmt);

	// Create a table to test with
	ExecuteCmdAndCheckODBC("SQLExecDirect (CREATE TABLE)", SQL_HANDLE_STMT, hstmt, SQLExecDirect, hstmt,
	                       (SQLCHAR *)"CREATE TABLE testtbl(t varchar(40))", SQL_NTS);

	// A simple query against the table, fetch column info
	ExecuteCmdAndCheckODBC("SQLExecDirect (SELECT)", SQL_HANDLE_STMT, hstmt, SQLExecDirect, hstmt,
	                       (SQLCHAR *)"SELECT * FROM testtbl", SQL_NTS);

	// Get column metadata
	SQLSMALLINT num_cols;
	ExecuteCmdAndCheckODBC("SQLNumResultCols", SQL_HANDLE_STMT, hstmt, SQLNumResultCols, hstmt, &num_cols);
	REQUIRE(num_cols == 1);

	// Retrieve metadata from the column
	METADATA_CHECK(hstmt, num_cols, "t", sizeof('t'), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);

	// Alter the table
	ExecuteCmdAndCheckODBC("SQLExecDirect (ALTER TABLE)", SQL_HANDLE_STMT, hstmt, SQLExecDirect, hstmt,
	                       (SQLCHAR *)"ALTER TABLE testtbl ALTER t SET DATA TYPE int", SQL_NTS);

	// Rerun the query to check if the metadata was updated
	ExecuteCmdAndCheckODBC("SQLExecDirect (SELECT)", SQL_HANDLE_STMT, hstmt, SQLExecDirect, hstmt,
	                       (SQLCHAR *)"SELECT * FROM testtbl", SQL_NTS);

	// Get column metadata
	SQLSMALLINT num_cols2;
	ExecuteCmdAndCheckODBC("SQLNumResultCols", SQL_HANDLE_STMT, hstmt, SQLNumResultCols, hstmt, &num_cols2);
	REQUIRE(num_cols2 == 1);

	// Retrieve metadata from the column
	METADATA_CHECK(hstmt, num_cols2, "t", sizeof('t'), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);

	ExecuteCmdAndCheckODBC("SQLFreeStmt (SQL_CLOSE)", SQL_HANDLE_STMT, hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);

	DISCONNECT_FROM_DATABASE(ret, env, dbc);
}
