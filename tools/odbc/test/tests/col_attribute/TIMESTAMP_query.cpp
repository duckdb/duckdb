#include "utils.h"

using namespace odbc_col_attribute_test;

TEST_CASE("Test SQLColAttribute TIMESTAMP", "[odbc][timestamp]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// run a simple query  with ints to get a result set
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT TIMESTAMP '1992-09-20 12:34:56.123456' as ts"), SQL_NTS);
	std::map<SQLLEN, ExpectedResult> expected_int;

	// https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolattribute-function?view=sql-server-ver16
	expected_int[SQL_DESC_COUNT] = ExpectedResult(1);
	expected_int[SQL_DESC_CATALOG_NAME] = ExpectedResult("system");
	expected_int[SQL_DESC_CASE_SENSITIVE] = ExpectedResult(SQL_FALSE);
	expected_int[SQL_DESC_FIXED_PREC_SCALE] = ExpectedResult(SQL_FALSE);
	expected_int[SQL_DESC_LITERAL_PREFIX] = ExpectedResult("''''"); // empty string?
	expected_int[SQL_DESC_LITERAL_SUFFIX] = ExpectedResult("''''"); // empty string?
	expected_int[SQL_DESC_LOCAL_TYPE_NAME] = ExpectedResult("");    // empty string!
	expected_int[SQL_DESC_NULLABLE] = ExpectedResult(SQL_NULLABLE);
	expected_int[SQL_DESC_NUM_PREC_RADIX] = ExpectedResult(0);          // 0 for all non-numeric data types
	expected_int[SQL_DESC_SCHEMA_NAME] = ExpectedResult("");            // If the data source does not support schemas
	                                                                    //   or the schema name cannot be determined,
	                                                                    //   an empty string is returned.
	expected_int[SQL_DESC_SEARCHABLE] = ExpectedResult(SQL_PRED_BASIC); // column can be used in a WHERE clause with
	                                                                    //   all the comparison operators except LIKE.
	expected_int[SQL_DESC_UNNAMED] =
	    ExpectedResult(SQL_NAMED); // If the SQL_DESC_NAME field of the IRD contains
	                               //   a column alias or a column name, SQL_NAMED is returned.
	expected_int[SQL_DESC_UNSIGNED] = ExpectedResult(SQL_TRUE); // SQL_TRUE if the column is unsigned
	expected_int[SQL_DESC_UPDATABLE] = ExpectedResult(SQL_ATTR_READONLY);

	// https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/column-size?view=sql-server-ver16
	expected_int[SQL_DESC_DISPLAY_SIZE] = ExpectedResult(20);

	// ODBC 2 - see ODBC3 sectiion below for details
	expected_int[SQL_COLUMN_LENGTH] = ExpectedResult(26);
	expected_int[SQL_COLUMN_PRECISION] = ExpectedResult(26);
	// expected_int[SQL_COLUMN_SCALE] = ExpectedResult(0);

	// ODBC 3
	expected_int[SQL_DESC_LENGTH] = ExpectedResult(26);    // A numeric value that is either the maximum
	                                                       //   or actual character length of a character
	                                                       //   string or binary data type.
	expected_int[SQL_DESC_PRECISION] = ExpectedResult(26); // A numeric value that for a numeric data type
	                                                       //   denotes the applicable precision. For data
	                                                       //   types SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP, and
	                                                       //   all the interval data types that represent a
	                                                       //   time interval, its value is the applicable precision
	                                                       //   of the fractional seconds component.

	expected_int[SQL_DESC_TYPE] =
	    ExpectedResult(SQL_DATETIME); // For the datetime and interval data types, this field returns
	                                  //   the verbose data type: SQL_DATETIME or SQL_INTERVAL.

	expected_int[SQL_DESC_TYPE_NAME] = ExpectedResult("INT64");

	expected_int[SQL_DESC_CONCISE_TYPE] =
	    ExpectedResult(SQL_TYPE_TIMESTAMP); // For the datetime and interval data types, this field returns
	                                        //   the concise data type; for example, SQL_TYPE_TIME or
	                                        //   SQL_INTERVAL_YEAR.

	TestAllFields(hstmt, expected_int);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}
