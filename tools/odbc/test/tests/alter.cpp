#include "../common.h"

using namespace odbc_test;

TEST_CASE("Alter", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;
	auto types_map = InitializeTypesMap();

	// Connect to the database
	CONNECT_TO_DATABASE(env, dbc);

	ExecuteCmdAndCheckODBC("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Create a table to test with
	ExecuteCmdAndCheckODBC("SQLExecDirect (CREATE TABLE)", SQLExecDirect, hstmt,
	                       ConvertToSQLCHAR("CREATE TABLE testtbl(t varchar(40))"), SQL_NTS);

	// A simple query against the table, fetch column info
	ExecuteCmdAndCheckODBC("SQLExecDirect (SELECT)", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT * FROM testtbl"),
	                       SQL_NTS);

	// Get column metadata
	SQLSMALLINT num_cols;
	ExecuteCmdAndCheckODBC("SQLNumResultCols", SQLNumResultCols, hstmt, &num_cols);
	REQUIRE(num_cols == 1);

	// Retrieve metadata from the column
	METADATA_CHECK(hstmt, num_cols, "t", sizeof('t'), SQL_VARCHAR, types_map[SQL_VARCHAR], 0, SQL_NULLABLE_UNKNOWN);

	// Alter the table
	ExecuteCmdAndCheckODBC("SQLExecDirect (ALTER TABLE)", SQLExecDirect, hstmt,
	                       ConvertToSQLCHAR("ALTER TABLE testtbl ALTER t SET DATA TYPE int"), SQL_NTS);

	// Rerun the query to check if the metadata was updated
	ExecuteCmdAndCheckODBC("SQLExecDirect (SELECT)", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT * FROM testtbl"),
	                       SQL_NTS);

	// Get column metadata
	SQLSMALLINT num_cols2;
	ExecuteCmdAndCheckODBC("SQLNumResultCols", SQLNumResultCols, hstmt, &num_cols2);
	REQUIRE(num_cols2 == 1);

	// Retrieve metadata from the column
	METADATA_CHECK(hstmt, num_cols2, "t", sizeof('t'), SQL_INTEGER, types_map[SQL_INTEGER], 0, SQL_NULLABLE_UNKNOWN);

	ExecuteCmdAndCheckODBC("SQLFreeStmt (SQL_CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
