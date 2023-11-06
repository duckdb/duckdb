#include "utils.h"

using namespace odbc_col_attribute_test;

TEST_CASE("Test SQLColAttribute for a query that returns an int", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// run a simple query  with ints to get a result set
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT 1 AS a, 2 AS b"), SQL_NTS);
	std::map<SQLLEN, ExpectedResult *> expected_int;
	expected_int[SQL_DESC_CASE_SENSITIVE] = new ExpectedResult(SQL_FALSE);
	expected_int[SQL_DESC_CATALOG_NAME] = new ExpectedResult("system");
	expected_int[SQL_DESC_CONCISE_TYPE] = new ExpectedResult(SQL_INTEGER);
	expected_int[SQL_DESC_COUNT] = new ExpectedResult(2);
	expected_int[SQL_DESC_DISPLAY_SIZE] = new ExpectedResult(11);
	expected_int[SQL_DESC_FIXED_PREC_SCALE] = new ExpectedResult(SQL_FALSE);
	expected_int[SQL_DESC_LENGTH] = new ExpectedResult(10);
	expected_int[SQL_DESC_LITERAL_PREFIX] = new ExpectedResult("NULL");
	expected_int[SQL_DESC_LITERAL_SUFFIX] = new ExpectedResult("NULL");
	expected_int[SQL_DESC_LOCAL_TYPE_NAME] = new ExpectedResult("");
	expected_int[SQL_DESC_NULLABLE] = new ExpectedResult(SQL_NULLABLE);
	expected_int[SQL_DESC_NUM_PREC_RADIX] = new ExpectedResult(2);
	expected_int[SQL_DESC_PRECISION] = new ExpectedResult(10);
	expected_int[SQL_COLUMN_SCALE] = new ExpectedResult(0);
	expected_int[SQL_DESC_SCALE] = new ExpectedResult(0);
	expected_int[SQL_DESC_SCHEMA_NAME] = new ExpectedResult("");
	expected_int[SQL_DESC_SEARCHABLE] = new ExpectedResult(SQL_PRED_BASIC);
	expected_int[SQL_DESC_TYPE] = new ExpectedResult(SQL_INTEGER);
	expected_int[SQL_DESC_UNNAMED] = new ExpectedResult(SQL_NAMED);
	expected_int[SQL_DESC_UNSIGNED] = new ExpectedResult(SQL_FALSE);
	expected_int[SQL_DESC_UPDATABLE] = new ExpectedResult(SQL_ATTR_READONLY);
	TestAllFields(hstmt, expected_int);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}
