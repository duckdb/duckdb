#include "../common.h"

#include <iostream>

using namespace odbc_test;

/* Tests the following catalog functions:
 * SQLGetTypeInfo
 * SQLTables
 *
 *
 */

void TestGetTypeInfo(HSTMT &hstmt) {
	SQLRETURN ret;
	SQLSMALLINT num_cols;

	// Check for SQLGetTypeInfo
	ret = SQLGetTypeInfo(hstmt, SQL_VARCHAR);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLGetTypeInfo");

	ret = SQLNumResultCols(hstmt, &num_cols);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLNumResultCols");
	REQUIRE(num_cols == 19);

	ret = SQLFetch(hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFetch");

	// Retrieve metadata & data from the column
	string col_name = "TYPE_NAME";
	METADATA_CHECK(hstmt, 1, col_name.c_str(), col_name.length(), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 1, "VARCHAR");

	col_name = "DATA_TYPE";
	METADATA_CHECK(hstmt, 2, col_name.c_str(), col_name.length(), SQL_SMALLINT, 5, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 2, "12");

	col_name = "COLUMN_SIZE";
	METADATA_CHECK(hstmt, 3, col_name.c_str(), col_name.length(), SQL_INTEGER, 11, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 3, "-1");

	col_name = "LITERAL_PREFIX";
	METADATA_CHECK(hstmt, 4, col_name.c_str(), col_name.length(), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 4, "'");

	col_name = "LITERAL_SUFFIX";
	METADATA_CHECK(hstmt, 5, col_name.c_str(), col_name.length(), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 5, "'");

	col_name = "CREATE_PARAMS";
	METADATA_CHECK(hstmt, 6, col_name.c_str(), col_name.length(), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 6, "length");

	col_name = "NULLABLE";
	METADATA_CHECK(hstmt, 7, col_name.c_str(), col_name.length(), SQL_SMALLINT, 5, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 7, "1");

	col_name = "CASE_SENSITIVE";
	METADATA_CHECK(hstmt, 8, col_name.c_str(), col_name.length(), SQL_SMALLINT, 5, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 8, "1");

	col_name = "SEARCHABLE";
	METADATA_CHECK(hstmt, 9, col_name.c_str(), col_name.length(), SQL_SMALLINT, 5, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 9, "3");

	col_name = "UNSIGNED_ATTRIBUTE";
	METADATA_CHECK(hstmt, 10, col_name.c_str(), col_name.length(), SQL_SMALLINT, 5, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 10, "-1");

	col_name = "FIXED_PREC_SCALE";
	METADATA_CHECK(hstmt, 11, col_name.c_str(), col_name.length(), SQL_SMALLINT, 5, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 11, "0");

	col_name = "AUTO_UNIQUE_VALUE";
	METADATA_CHECK(hstmt, 12, col_name.c_str(), col_name.length(), SQL_SMALLINT, 5, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 12, "-1");

	col_name = "LOCAL_TYPE_NAME";
	METADATA_CHECK(hstmt, 13, col_name.c_str(), col_name.length(), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 13, nullptr);

	col_name = "MINIMUM_SCALE";
	METADATA_CHECK(hstmt, 14, col_name.c_str(), col_name.length(), SQL_SMALLINT, 5, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 14, "-1");

	col_name = "MAXIMUM_SCALE";
	METADATA_CHECK(hstmt, 15, col_name.c_str(), col_name.length(), SQL_SMALLINT, 5, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 15, "-1");

	col_name = "SQL_DATA_TYPE";
	METADATA_CHECK(hstmt, 16, col_name.c_str(), col_name.length(), SQL_SMALLINT, 5, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 16, "12");

	col_name = "SQL_DATETIME_SUB";
	METADATA_CHECK(hstmt, 17, col_name.c_str(), col_name.length(), SQL_SMALLINT, 5, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 17, "-1");

	col_name = "NUM_PREC_RADIX";
	METADATA_CHECK(hstmt, 18, col_name.c_str(), col_name.length(), SQL_INTEGER, 11, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 18, "-1");

	col_name = "INTERVAL_PRECISION";
	METADATA_CHECK(hstmt, 19, col_name.c_str(), col_name.length(), SQL_SMALLINT, 5, 0, SQL_NULLABLE_UNKNOWN);
	DATA_CHECK(hstmt, 19, "-1");
}

static void TestSQLTables(HSTMT &hstmt) {
	SQLRETURN ret;

	ret = SQLTables(hstmt, NULL, 0, (SQLCHAR *)"main", SQL_NTS, (SQLCHAR *)"%", SQL_NTS, (SQLCHAR *)"TABLE", SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLTables");

	SQLSMALLINT col_count;

	ret = SQLNumResultCols(hstmt, &col_count);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLNumResultCols");
	REQUIRE(col_count == 5);

	string col_name = "TABLE_CAT";
	METADATA_CHECK(hstmt, 1, col_name.c_str(), col_name.length(), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);

	col_name = "TABLE_SCHEM";
	METADATA_CHECK(hstmt, 2, col_name.c_str(), col_name.length(), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);

	col_name = "TABLE_NAME";
	METADATA_CHECK(hstmt, 3, col_name.c_str(), col_name.length(), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);

	col_name = "TABLE_TYPE";
	METADATA_CHECK(hstmt, 4, col_name.c_str(), col_name.length(), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);

	col_name = "REMARKS";
	METADATA_CHECK(hstmt, 5, col_name.c_str(), col_name.length(), SQL_VARCHAR, 256, 0, SQL_NULLABLE_UNKNOWN);

	int fetch_count = 0;
	do {
		ret = SQLFetch(hstmt);
		if (ret == SQL_NO_DATA) {
			break;
		}
		ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFetch");
		fetch_count++;

		DATA_CHECK(hstmt, 1, "memory");
		DATA_CHECK(hstmt, 2, "main");

		switch (fetch_count) {
		case 1:
			DATA_CHECK(hstmt, 3, "bool_table");
			break;
		case 2:
			DATA_CHECK(hstmt, 3, "byte_table");
			break;
		case 3:
			DATA_CHECK(hstmt, 3, "interval_table");
			break;
		case 4:
			DATA_CHECK(hstmt, 3, "lo_test_table");
			break;
		case 5:
			DATA_CHECK(hstmt, 3, "test_table_1");
		}

		DATA_CHECK(hstmt, 4, "TABLE");
	} while (ret == SQL_SUCCESS);
}

TEST_CASE("catalog_functions", "[odbc") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(ret, env, dbc);

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLAllocHandle (HSTMT)");

	// Initializes the database with dummy data
	INITIALIZE_DATABASE(hstmt);

	SQLExecDirect(hstmt, (SQLCHAR *)"DROP TABLE IF EXISTS test_table", SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLExecDirect (DROP TABLE)");

	// Check for SQLGetTypeInfo
	TestGetTypeInfo(hstmt);

	// Check for SQLTables
	TestSQLTables(hstmt);

	// Free the statement handle
	ret = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFreeHandle (HSTMT)");

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(ret, dbc, env);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect (HDBC)");
}

// SELECT "LOCAL_TYPE_NAME" FROM (SELECT * FROM (VALUES (CAST('VARCHAR' AS VARCHAR),CAST(12 AS SMALLINT),CAST(-1 AS
// INTEGER),CAST('''' AS VARCHAR),CAST('''' AS VARCHAR),CAST('length' AS VARCHAR),CAST(1 AS SMALLINT),CAST(1 AS
// SMALLINT),CAST(3 AS SMALLINT),CAST(-1 AS SMALLINT),CAST(0 AS SMALLINT),CAST(-1 AS SMALLINT),CAST(NULL AS
// VARCHAR),CAST(-1 AS SMALLINT),CAST(-1 AS SMALLINT),CAST(12 AS SMALLINT),CAST(-1 AS SMALLINT),CAST(-1 AS
// integer),CAST(-1 AS SMALLINT))) AS odbc_types ("TYPE_NAME", "DATA_TYPE", "COLUMN_SIZE", "LITERAL_PREFIX",
// "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
// "FIXED_PREC_SCALE", "AUTO_UNIQUE_VALUE", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE",
// "SQL_DATETIME_SUB", "NUM_PREC_RADIX", "INTERVAL_PRECISION")) as result;
