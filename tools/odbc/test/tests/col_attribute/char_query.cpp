#include "utils.h"

using namespace odbc_col_attribute_test;

TEST_CASE("Test SQLColAttribute for a query that returns a char", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// run a simple query with chars to get a result set
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT 'a' AS a, 'b' AS b"), SQL_NTS);
	std::map<SQLLEN, ExpectedResult> expected_chars;
	expected_chars[SQL_DESC_CASE_SENSITIVE] = ExpectedResult(SQL_TRUE);
	expected_chars[SQL_DESC_CATALOG_NAME] = ExpectedResult("system");
	expected_chars[SQL_DESC_CONCISE_TYPE] = ExpectedResult(SQL_VARCHAR);
	expected_chars[SQL_DESC_COUNT] = ExpectedResult(2);
	expected_chars[SQL_DESC_DISPLAY_SIZE] = ExpectedResult(256);
	expected_chars[SQL_DESC_FIXED_PREC_SCALE] = ExpectedResult(SQL_FALSE);
	expected_chars[SQL_DESC_LENGTH] = ExpectedResult(-1);
	expected_chars[SQL_DESC_LITERAL_PREFIX] = ExpectedResult("''''");
	expected_chars[SQL_DESC_LITERAL_SUFFIX] = ExpectedResult("''''");
	expected_chars[SQL_DESC_LOCAL_TYPE_NAME] = ExpectedResult("");
	expected_chars[SQL_DESC_NULLABLE] = ExpectedResult(SQL_NULLABLE);
	expected_chars[SQL_DESC_NUM_PREC_RADIX] = ExpectedResult(0);
	expected_chars[SQL_DESC_PRECISION] = ExpectedResult(-1);
	expected_chars[SQL_COLUMN_SCALE] = ExpectedResult(SQL_NO_TOTAL);
	expected_chars[SQL_DESC_SCALE] = ExpectedResult(SQL_NO_TOTAL);
	expected_chars[SQL_DESC_SCHEMA_NAME] = ExpectedResult("");
	expected_chars[SQL_DESC_SEARCHABLE] = ExpectedResult(SQL_SEARCHABLE);
	expected_chars[SQL_DESC_TYPE] = ExpectedResult(SQL_VARCHAR);
	expected_chars[SQL_DESC_UNNAMED] = ExpectedResult(SQL_NAMED);
	expected_chars[SQL_DESC_UNSIGNED] = ExpectedResult(SQL_TRUE);
	expected_chars[SQL_DESC_UPDATABLE] = ExpectedResult(SQL_ATTR_READONLY);
	TestAllFields(hstmt, expected_chars);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}
