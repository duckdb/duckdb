#include "../common.h"

using namespace odbc_test;

TEST_CASE("Test SQLColAttribute (descriptor information for a column)", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Get column attributes of a simple query
	EXECUTE_AND_CHECK("SQLExectDirect", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT "
	                                   "'1'::int AS intcol, "
	                                   "'foobar'::text AS textcol, "
	                                   "'varchar string'::varchar as varcharcol, "
	                                   "''::varchar as empty_varchar_col, "
	                                   "'varchar-5-col'::varchar(5) as varchar5col, "
	                                   "'5 days'::interval day to second"),
	                  SQL_NTS);

	// Get the number of columns
	SQLSMALLINT num_cols;
	EXECUTE_AND_CHECK("SQLNumResultCols", SQLNumResultCols, hstmt, &num_cols);
	REQUIRE(num_cols == 6);

	// Loop through the columns
	for (int i = 1; i <= num_cols; i++) {
		char buffer[64];
		SQLLEN number;

		// Get the column label
		EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, hstmt, i, SQL_DESC_LABEL, buffer, sizeof(buffer), nullptr,
		                  nullptr);
		switch (i) {
		case 1:
			REQUIRE(STR_EQUAL(buffer, "intcol"));
			break;
		case 2:
			REQUIRE(STR_EQUAL(buffer, "textcol"));
			break;
		case 3:
			REQUIRE(STR_EQUAL(buffer, "varcharcol"));
			break;
		case 4:
			REQUIRE(STR_EQUAL(buffer, "empty_varchar_col"));
			break;
		case 5:
			REQUIRE(STR_EQUAL(buffer, "varchar5col"));
			break;
		case 6:
			REQUIRE(STR_EQUAL(buffer, "CAST('5 days' AS INTERVAL)"));
			break;
		}

		// Get the column octet length
		EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, hstmt, i, SQL_DESC_OCTET_LENGTH, nullptr, SQL_IS_INTEGER,
		                  nullptr, &number);
		REQUIRE(number == 0);

		// Get the column type name
		EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, hstmt, i, SQL_DESC_TYPE_NAME, buffer, sizeof(buffer),
		                  nullptr, nullptr);
		switch (i) {
		case 1:
			REQUIRE(STR_EQUAL(buffer, "INT32"));
			break;
		case 2:
		case 3:
		case 4:
		case 5:
			REQUIRE(STR_EQUAL(buffer, "VARCHAR"));
			break;
		case 6:
			REQUIRE(STR_EQUAL(buffer, "INTERVAL"));
			break;
		}
	}

	// SQLColAttribute should fail if the column number is out of bounds
	ret = SQLColAttribute(hstmt, 7, SQL_DESC_TYPE_NAME, nullptr, 0, nullptr, nullptr);
	REQUIRE(ret == SQL_ERROR);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}
