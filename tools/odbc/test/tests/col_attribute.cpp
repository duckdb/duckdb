#include "../common.h"

#include <iostream>

using namespace odbc_test;

TEST_CASE("colatribute", "[odbc") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(ret, env, dbc);

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLAllocHandle (HSTMT)");

	// Get column attributes of a simple query
	ret = SQLExecDirect(hstmt,
	                    (SQLCHAR *)"SELECT "
	                               "'1'::int AS intcol, "
	                               "'foobar'::text AS textcol, "
	                               "'varchar string'::varchar as varcharcol, "
	                               "''::varchar as empty_varchar_col, "
	                               "'varchar-5-col'::varchar(5) as varchar5col, "
	                               "'5 days'::interval day to second",
	                    SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLExecDirect");

	// Get the number of columns
	SQLSMALLINT num_cols;
	ret = SQLNumResultCols(hstmt, &num_cols);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLNumResultCols");

	REQUIRE(num_cols == 6);

	// Loop through the columns
	for (int i = 1; i <= num_cols; i++) {
		char buffer[64];
		SQLLEN number;

		// Get the column label
		ret = SQLColAttribute(hstmt, i, SQL_DESC_LABEL, buffer, sizeof(buffer), nullptr, nullptr);
		ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLColAttribute");
		switch (i) {
		case 1:
			REQUIRE(strcmp(buffer, "intcol") == 0);
			break;
		case 2:
			REQUIRE(strcmp(buffer, "textcol") == 0);
			break;
		case 3:
			REQUIRE(strcmp(buffer, "varcharcol") == 0);
			break;
		case 4:
			REQUIRE(strcmp(buffer, "empty_varchar_col") == 0);
			break;
		case 5:
			REQUIRE(strcmp(buffer, "varchar5col") == 0);
			break;
		case 6:
			REQUIRE(strcmp(buffer, "CAST('5 days' AS INTERVAL)") == 0);
			break;
		}

		// Get the column octet length
		ret = SQLColAttribute(hstmt, i, SQL_DESC_OCTET_LENGTH, nullptr, SQL_IS_INTEGER, nullptr, &number);
		ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLColAttribute");
		REQUIRE(number == 0);

		// Get the column type name
		ret = SQLColAttribute(hstmt, i, SQL_DESC_TYPE_NAME, buffer, sizeof(buffer), nullptr, nullptr);
		ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLColAttribute");
		switch (i) {
		case 1:
			REQUIRE(strcmp(buffer, "INT32") == 0);
			break;
		case 2:
		case 3:
		case 4:
		case 5:
			REQUIRE(strcmp(buffer, "VARCHAR") == 0);
			break;
		case 6:
			REQUIRE(strcmp(buffer, "INTERVAL") == 0);
			break;
		}
	}

	// SQLColAttribute should fail if the column number is out of bounds
	ret = SQLColAttribute(hstmt, 7, SQL_DESC_TYPE_NAME, nullptr, 0, nullptr, nullptr);
	REQUIRE(ret == SQL_ERROR);

	// Free the statement handle
	ret = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFreeHandle (HSTMT)");

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(ret, dbc, env);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect (HDBC)");
}
