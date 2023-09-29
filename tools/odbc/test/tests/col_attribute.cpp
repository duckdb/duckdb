#include "../common.h"

using namespace odbc_test;

template <typename FUNC>
static void CheckString(FUNC func, SQLHANDLE handle, const std::string &expected, SQLSMALLINT field_identifier) {
	SQLCHAR buffer[64];
	EXECUTE_AND_CHECK("SQLColAttribute", func, handle, 1, field_identifier, buffer, sizeof(buffer), nullptr, nullptr);
	REQUIRE(ConvertToString(buffer) == expected);
}

template <typename FUNC>
static void CheckInteger(FUNC func, SQLHANDLE handle, SQLLEN expected, SQLSMALLINT field_identifier) {
	SQLLEN number;
	EXECUTE_AND_CHECK("SQLColAttribute", func, handle, 1, field_identifier, nullptr, 0, nullptr, &number);
	REQUIRE(number == expected);
}

template <typename FUNC>
static void ExpectError(FUNC func, SQLHANDLE handle, SQLSMALLINT field_identifier) {
	SQLRETURN ret = func(handle, 1, field_identifier, nullptr, sizeof(nullptr), nullptr, nullptr);
	REQUIRE(ret == SQL_ERROR);
}

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

		// Get the column label
		EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, hstmt, i, SQL_DESC_LABEL, buffer, sizeof(buffer), nullptr,
		                  nullptr);

		// Get the column name and base column name
		char col_name[64];
		char base_col_name[64];
		EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, hstmt, i, SQL_DESC_NAME, col_name, sizeof(col_name),
		                  nullptr, nullptr);
		EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, hstmt, i, SQL_DESC_BASE_COLUMN_NAME, base_col_name,
		                  sizeof(base_col_name), nullptr, nullptr);
		REQUIRE(STR_EQUAL(col_name, base_col_name));
		switch (i) {
		case 1:
			REQUIRE(STR_EQUAL(buffer, "intcol"));
			REQUIRE(STR_EQUAL(col_name, "intcol"));
			REQUIRE(STR_EQUAL(base_col_name, "intcol"));
			break;
		case 2:
			REQUIRE(STR_EQUAL(buffer, "textcol"));
			REQUIRE(STR_EQUAL(col_name, "textcol"));
			REQUIRE(STR_EQUAL(base_col_name, "textcol"));
			break;
		case 3:
			REQUIRE(STR_EQUAL(buffer, "varcharcol"));
			REQUIRE(STR_EQUAL(col_name, "varcharcol"));
			REQUIRE(STR_EQUAL(base_col_name, "varcharcol"));
			break;
		case 4:
			REQUIRE(STR_EQUAL(buffer, "empty_varchar_col"));
			REQUIRE(STR_EQUAL(col_name, "empty_varchar_col"));
			REQUIRE(STR_EQUAL(base_col_name, "empty_varchar_col"));
			break;
		case 5:
			REQUIRE(STR_EQUAL(buffer, "varchar5col"));
			REQUIRE(STR_EQUAL(col_name, "varchar5col"));
			REQUIRE(STR_EQUAL(base_col_name, "varchar5col"));
			break;
		case 6:
			REQUIRE(STR_EQUAL(buffer, "CAST('5 days' AS INTERVAL)"));
			REQUIRE(STR_EQUAL(col_name, "CAST('5 days' AS INTERVAL)"));
			REQUIRE(STR_EQUAL(base_col_name, "CAST('5 days' AS INTERVAL)"));
			break;
		}

		// Get the column octet length
		CheckInteger(SQLColAttribute, hstmt, 0, SQL_DESC_OCTET_LENGTH);

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

	// run simple query to get a result set
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT 1 AS a, 2 AS b"), SQL_NTS);

	// SQL_DESC_AUTO_UNIQUE_VALUE
	SQLLEN is_auto_incremental;
	ret = SQLColAttribute(hstmt, 1, SQL_DESC_AUTO_UNIQUE_VALUE, nullptr, 0, nullptr, &is_auto_incremental);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
	REQUIRE(is_auto_incremental == SQL_FALSE);

	// SQL_DESC_BASE_TABLE_NAME
	ExpectError(SQLColAttribute, hstmt, SQL_DESC_BASE_TABLE_NAME);

	// SQL_DESC_CASE_SENSITIVE
	CheckInteger(SQLColAttribute, hstmt, SQL_FALSE, SQL_DESC_CASE_SENSITIVE);

	// SQL_DESC_CATALOG_NAME
	CheckString(SQLColAttribute, hstmt, "system", SQL_DESC_CATALOG_NAME);

	// SQL_DESC_CONCISE_TYPE
	CheckInteger(SQLColAttribute, hstmt, SQL_INTEGER, SQL_DESC_CONCISE_TYPE);

	// SQL_DESC_COUNT
	CheckInteger(SQLColAttribute, hstmt, 2, SQL_DESC_COUNT);

	// SQL_DESC_DISPLAY_SIZE
	CheckInteger(SQLColAttribute, hstmt, 11, SQL_DESC_DISPLAY_SIZE);

	// SQL_DESC_FIXED_PREC_SCALE
	CheckInteger(SQLColAttribute, hstmt, SQL_FALSE, SQL_DESC_FIXED_PREC_SCALE);

	// SQL_DESC_LENGTH
	CheckInteger(SQLColAttribute, hstmt, 10, SQL_DESC_LENGTH);

	// SQL_DESC_LITERAL_PREFIX
	CheckString(SQLColAttribute, hstmt, "NULL", SQL_DESC_LITERAL_PREFIX);

	// SQL_DESC_LITERAL_SUFFIX
	CheckString(SQLColAttribute, hstmt, "NULL", SQL_DESC_LITERAL_SUFFIX);

	// SQL_DESC_LOCAL_TYPE_NAME
	CheckString(SQLColAttribute, hstmt, "", SQL_DESC_LOCAL_TYPE_NAME);

	// SQL_DESC_NULLABLE
	CheckInteger(SQLColAttribute, hstmt, SQL_NULLABLE, SQL_DESC_NULLABLE);

	// SQL_DESC_NUM_PREC_RADIX
	CheckInteger(SQLColAttribute, hstmt, 2, SQL_DESC_NUM_PREC_RADIX);

	// SQL_DESC_PRECISION
	CheckInteger(SQLColAttribute, hstmt, 10, SQL_DESC_PRECISION);

	// SQL_DESC_SCALE
	CheckInteger(SQLColAttribute, hstmt, 0, SQL_DESC_SCALE);

	// SQL_DESC_SCHEMA_NAME
	CheckString(SQLColAttribute, hstmt, "", SQL_DESC_SCHEMA_NAME);

	// SQL_DESC_SEARCHABLE
	CheckInteger(SQLColAttribute, hstmt, SQL_PRED_BASIC, SQL_DESC_SEARCHABLE);

	// SQL_DESC_TYPE
	CheckInteger(SQLColAttribute, hstmt, SQL_INTEGER, SQL_DESC_TYPE);

	// SQL_DESC_UNNAMED
	CheckInteger(SQLColAttribute, hstmt, SQL_NAMED, SQL_DESC_UNNAMED);

	// SQL_DESC_UNSIGNED
	CheckInteger(SQLColAttribute, hstmt, SQL_FALSE, SQL_DESC_UNSIGNED);

	// SQL_DESC_UPDATABLE
	CheckInteger(SQLColAttribute, hstmt, SQL_ATTR_READONLY, SQL_DESC_UPDATABLE);

	// SQLColAttribute should fail if the column number is out of bounds
	ret = SQLColAttribute(hstmt, 7, SQL_DESC_TYPE_NAME, nullptr, 0, nullptr, nullptr);
	REQUIRE(ret == SQL_ERROR);

	// Create table and retrieve base table name using SQLColAttribute, should fail because the statement is not a
	// SELECT
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE test (a INTEGER, b INTEGER)"), SQL_NTS);
	ExpectError(SQLColAttribute, hstmt, SQL_DESC_BASE_TABLE_NAME);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}
