#include "../common.h"

using namespace odbc_test;

class ExpectedResult {
public:
	std::string s;
	SQLLEN n;

	explicit ExpectedResult(SQLLEN n_n) : n(n_n) {};

	explicit ExpectedResult(const std::string &n_s) : s(n_s) {};

	ExpectedResult(const ExpectedResult &other) {
		*this = other;
	}
};

static void DeleteExpectedMap(std::map<SQLLEN, ExpectedResult *> &expected) {
	for (auto &it : expected) {
		delete it.second;
	}
}

static void CheckString(SQLHANDLE handle, const std::string &expected, SQLSMALLINT field_identifier) {
	SQLCHAR buffer[64];
	EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, handle, 1, field_identifier, buffer, sizeof(buffer), nullptr,
	                  nullptr);
	REQUIRE(ConvertToString(buffer) == expected);
}

static void CheckInteger(SQLHANDLE handle, SQLLEN expected, SQLSMALLINT field_identifier) {
	SQLLEN number;
	EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, handle, 1, field_identifier, nullptr, 0, nullptr, &number);
	REQUIRE(number == expected);
}

static void ExpectError(SQLHANDLE handle, SQLSMALLINT field_identifier) {
	SQLRETURN ret = SQLColAttribute(handle, 1, field_identifier, nullptr, sizeof(nullptr), nullptr, nullptr);
	REQUIRE(ret == SQL_ERROR);
}

static void TestAllFields(SQLHANDLE hstmt, std::map<SQLLEN, ExpectedResult *> expected) {
	// SQL_DESC_AUTO_UNIQUE_VALUE
	SQLLEN n;
	SQLRETURN ret = SQLColAttribute(hstmt, 1, SQL_DESC_AUTO_UNIQUE_VALUE, nullptr, 0, nullptr, &n);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
	REQUIRE(n == SQL_FALSE);

	// SQL_DESC_BASE_TABLE_NAME
	ExpectError(hstmt, SQL_DESC_BASE_TABLE_NAME);

	// SQL_DESC_CASE_SENSITIVE
	// [expected int: SQL_FALSE, char: SQL_TRUE]
	CheckInteger(hstmt, expected[SQL_DESC_CASE_SENSITIVE]->n, SQL_DESC_CASE_SENSITIVE);

	// SQL_DESC_CATALOG_NAME
	// [expected: int: "system", char: "system"]
	CheckString(hstmt, expected[SQL_DESC_CATALOG_NAME]->s, SQL_DESC_CATALOG_NAME);

	// SQL_DESC_CONCISE_TYPE
	// [expected: int: SQL_INTEGER, char: SQL_VARCHAR]
	CheckInteger(hstmt, expected[SQL_DESC_CONCISE_TYPE]->n, SQL_DESC_CONCISE_TYPE);

	// SQL_DESC_COUNT
	// [expected: int: 2, char: 2]
	CheckInteger(hstmt, expected[SQL_DESC_COUNT]->n, SQL_DESC_COUNT);

	// SQL_DESC_DISPLAY_SIZE
	// [expected: int 11, char 256]
	CheckInteger(hstmt, expected[SQL_DESC_DISPLAY_SIZE]->n, SQL_DESC_DISPLAY_SIZE);

	// SQL_DESC_FIXED_PREC_SCALE
	// [expected: int: SQL_FALSE, char: SQL_FALSE]
	CheckInteger(hstmt, expected[SQL_DESC_FIXED_PREC_SCALE]->n, SQL_DESC_FIXED_PREC_SCALE);

	// SQL_DESC_LENGTH
	// [expected: int: 10, char: -1]
	CheckInteger(hstmt, expected[SQL_DESC_LENGTH]->n, SQL_DESC_LENGTH);

	// SQL_DESC_LITERAL_PREFIX
	// [expected: int: "NULL", char: "''''"]
	CheckString(hstmt, expected[SQL_DESC_LITERAL_PREFIX]->s, SQL_DESC_LITERAL_PREFIX);

	// SQL_DESC_LITERAL_SUFFIX
	// [expected: int: "NULL", char: "''''"]
	CheckString(hstmt, expected[SQL_DESC_LITERAL_SUFFIX]->s, SQL_DESC_LITERAL_SUFFIX);

	// SQL_DESC_LOCAL_TYPE_NAME
	// [expected: int: "", char: ""]
	CheckString(hstmt, expected[SQL_DESC_LOCAL_TYPE_NAME]->s, SQL_DESC_LOCAL_TYPE_NAME);

	// SQL_DESC_NULLABLE
	// [expected: int: SQL_NULLABLE, char: SQL_NULLABLE]
	CheckInteger(hstmt, expected[SQL_DESC_NULLABLE]->n, SQL_DESC_NULLABLE);

	// SQL_DESC_NUM_PREC_RADIX
	// [expected: int: 2, char: 0]
	CheckInteger(hstmt, expected[SQL_DESC_NUM_PREC_RADIX]->n, SQL_DESC_NUM_PREC_RADIX);

	// SQL_DESC_PRECISION
	// [expected: int: 10, char: -1]
	CheckInteger(hstmt, expected[SQL_DESC_PRECISION]->n, SQL_DESC_PRECISION);

	// SQL_NO_TOTAL -> Returns SQL_SUCCESS_WITH_INFO
	if (expected[SQL_COLUMN_SCALE]->n == SQL_NO_TOTAL) {
		// SQL_COLUMN_SCALE
		// [expected: int: 0, char: SQL_NO_TOTAL]
		ret = SQLColAttribute(hstmt, 1, SQL_COLUMN_SCALE, nullptr, 0, nullptr, &n);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		REQUIRE(n == expected[SQL_COLUMN_SCALE]->n);

		// SQL_DESC_SCALE
		// [expected: int: 0, char: SQL_NO_TOTAL]
		ret = SQLColAttribute(hstmt, 1, SQL_DESC_SCALE, nullptr, 0, nullptr, &n);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		REQUIRE(n == expected[SQL_DESC_SCALE]->n);
	} else {
		CheckInteger(hstmt, expected[SQL_COLUMN_SCALE]->n, SQL_COLUMN_SCALE);
		CheckInteger(hstmt, expected[SQL_DESC_SCALE]->n, SQL_DESC_SCALE);
	}
	// SQL_DESC_SCHEMA_NAME
	// [expected: int: "", char: ""]
	CheckString(hstmt, expected[SQL_DESC_SCHEMA_NAME]->s, SQL_DESC_SCHEMA_NAME);

	// SQL_DESC_SEARCHABLE
	// [expected: int: SQL_PRED_BASIC, char: SQL_SEARCHABLE]
	CheckInteger(hstmt, expected[SQL_DESC_SEARCHABLE]->n, SQL_DESC_SEARCHABLE);

	// SQL_DESC_TYPE
	// [expected: int: SQL_INTEGER, char: SQL_VARCHAR]
	CheckInteger(hstmt, expected[SQL_DESC_TYPE]->n, SQL_DESC_TYPE);

	// SQL_DESC_UNNAMED
	// [expected: int: SQL_NAMED, char: SQL_NAMED]
	CheckInteger(hstmt, expected[SQL_DESC_UNNAMED]->n, SQL_DESC_UNNAMED);

	// SQL_DESC_UNSIGNED
	// [expected: int: SQL_FALSE, char: SQL_TRUE]
	CheckInteger(hstmt, expected[SQL_DESC_UNSIGNED]->n, SQL_DESC_UNSIGNED);

	// SQL_DESC_UPDATABLE
	// [expected: int: SQL_ATTR_READONLY, char: SQL_ATTR_READONLY]
	CheckInteger(hstmt, expected[SQL_DESC_UPDATABLE]->n, SQL_DESC_UPDATABLE);

	DeleteExpectedMap(expected);
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
		CheckInteger(hstmt, 0, SQL_DESC_OCTET_LENGTH);

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

	// run simple query  with ints to get a result set
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

	// run a simple query with chars to get a result set
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT 'a' AS a, 'b' AS b"), SQL_NTS);
	std::map<SQLLEN, ExpectedResult *> expected_chars;
	expected_chars[SQL_DESC_CASE_SENSITIVE] = new ExpectedResult(SQL_TRUE);
	expected_chars[SQL_DESC_CATALOG_NAME] = new ExpectedResult("system");
	expected_chars[SQL_DESC_CONCISE_TYPE] = new ExpectedResult(SQL_VARCHAR);
	expected_chars[SQL_DESC_COUNT] = new ExpectedResult(2);
	expected_chars[SQL_DESC_DISPLAY_SIZE] = new ExpectedResult(256);
	expected_chars[SQL_DESC_FIXED_PREC_SCALE] = new ExpectedResult(SQL_FALSE);
	expected_chars[SQL_DESC_LENGTH] = new ExpectedResult(-1);
	expected_chars[SQL_DESC_LITERAL_PREFIX] = new ExpectedResult("''''");
	expected_chars[SQL_DESC_LITERAL_SUFFIX] = new ExpectedResult("''''");
	expected_chars[SQL_DESC_LOCAL_TYPE_NAME] = new ExpectedResult("");
	expected_chars[SQL_DESC_NULLABLE] = new ExpectedResult(SQL_NULLABLE);
	expected_chars[SQL_DESC_NUM_PREC_RADIX] = new ExpectedResult(0);
	expected_chars[SQL_DESC_PRECISION] = new ExpectedResult(-1);
	expected_chars[SQL_COLUMN_SCALE] = new ExpectedResult(SQL_NO_TOTAL);
	expected_chars[SQL_DESC_SCALE] = new ExpectedResult(SQL_NO_TOTAL);
	expected_chars[SQL_DESC_SCHEMA_NAME] = new ExpectedResult("");
	expected_chars[SQL_DESC_SEARCHABLE] = new ExpectedResult(SQL_SEARCHABLE);
	expected_chars[SQL_DESC_TYPE] = new ExpectedResult(SQL_VARCHAR);
	expected_chars[SQL_DESC_UNNAMED] = new ExpectedResult(SQL_NAMED);
	expected_chars[SQL_DESC_UNSIGNED] = new ExpectedResult(SQL_TRUE);
	expected_chars[SQL_DESC_UPDATABLE] = new ExpectedResult(SQL_ATTR_READONLY);
	TestAllFields(hstmt, expected_chars);

	// SQLColAttribute should fail if the column number is out of bounds
	ret = SQLColAttribute(hstmt, 7, SQL_DESC_TYPE_NAME, nullptr, 0, nullptr, nullptr);
	REQUIRE(ret == SQL_ERROR);

	// Create table and retrieve base table name using SQLColAttribute, should fail because the statement is not a
	// SELECT
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE test (a INTEGER, b INTEGER)"), SQL_NTS);
	ExpectError(hstmt, SQL_DESC_BASE_TABLE_NAME);

	// Prepare a statement and call SQLColAttribute, succeeds but is undefined
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("create table colattrfoo(col1 int, col2 varchar(20))"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLPrepare", SQLPrepare, hstmt, ConvertToSQLCHAR("select * From colattrfoo"), SQL_NTS);

	SQLLEN fixed_prec_scale;
	EXECUTE_AND_CHECK("SQLColAttribute", SQLColAttribute, hstmt, 1, SQL_DESC_FIXED_PREC_SCALE, nullptr, 0, nullptr,
	                  &fixed_prec_scale);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}
