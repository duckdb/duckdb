#include <array>
#include <vector>
#include "../common.h"

using namespace odbc_test;

/* Tests the following catalog functions:
 * SQLGetTypeInfo
 * SQLTables
 * SQLColumns
 * SQLGetInfo
 *
 * TODO: Test the following catalog functions:
 * - SQLSpecialColumns
 * - SQLStatistics
 * - SQLPrimaryKeys
 * - SQLForeignKeys
 * - SQLProcedureColumns
 * - SQLTablePrivileges
 * - SQLColumnPrivileges
 * - SQLProcedures
 */

void TestGetTypeInfo(HSTMT &hstmt, std::map<SQLSMALLINT, SQLULEN> &types_map) {
	SQLSMALLINT col_count;

	// Check for SQLGetTypeInfo
	EXECUTE_AND_CHECK("SQLGetTypeInfo", SQLGetTypeInfo, hstmt, SQL_VARCHAR);

	EXECUTE_AND_CHECK("SQLNumResultCols", SQLNumResultCols, hstmt, &col_count);
	REQUIRE(col_count == 19);

	EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);

	std::vector<std::pair<MetadataData, std::string>> expected_data = {
	    {{"TYPE_NAME", SQL_VARCHAR}, "VARCHAR"},     {{"DATA_TYPE", SQL_SMALLINT}, "12"},
	    {{"COLUMN_SIZE", SQL_INTEGER}, "-1"},        {{"LITERAL_PREFIX", SQL_VARCHAR}, "'"},
	    {{"LITERAL_SUFFIX", SQL_VARCHAR}, "'"},      {{"CREATE_PARAMS", SQL_VARCHAR}, "length"},
	    {{"NULLABLE", SQL_SMALLINT}, "1"},           {{"CASE_SENSITIVE", SQL_SMALLINT}, "1"},
	    {{"SEARCHABLE", SQL_SMALLINT}, "3"},         {{"UNSIGNED_ATTRIBUTE", SQL_SMALLINT}, "-1"},
	    {{"FIXED_PREC_SCALE", SQL_SMALLINT}, "0"},   {{"AUTO_UNIQUE_VALUE", SQL_SMALLINT}, "-1"},
	    {{"LOCAL_TYPE_NAME", SQL_VARCHAR}, ""},      {{"MINIMUM_SCALE", SQL_SMALLINT}, "-1"},
	    {{"MAXIMUM_SCALE", SQL_SMALLINT}, "-1"},     {{"SQL_DATA_TYPE", SQL_SMALLINT}, "12"},
	    {{"SQL_DATETIME_SUB", SQL_SMALLINT}, "-1"},  {{"NUM_PREC_RADIX", SQL_INTEGER}, "-1"},
	    {{"INTERVAL_PRECISION", SQL_SMALLINT}, "-1"}};

	for (int i = 0; i < col_count; i++) {
		auto &entry = expected_data[i].first;
		METADATA_CHECK(hstmt, i + 1, entry.col_name.c_str(), entry.col_name.length(), entry.col_type,
		               types_map[entry.col_type], 0, SQL_NULLABLE_UNKNOWN);
		DATA_CHECK(hstmt, i + 1, expected_data[i].second);
	}

	// Test SQLGetTypeInfo with SQL_ALL_TYPES and data_type
	SQLINTEGER data_type;
	SQLLEN row_count = 0;
	SQLLEN len_or_ind_ptr;
	EXECUTE_AND_CHECK("SQLBindCol", SQLBindCol, hstmt, 2, SQL_INTEGER, &data_type, sizeof(data_type), &len_or_ind_ptr);
	EXECUTE_AND_CHECK("SQLGetTypeInfo(SQL_ALL_TYPES)", SQLGetTypeInfo, hstmt, SQL_ALL_TYPES);

	SQLINTEGER data_types[] = {
	    SQL_CHAR,
	    SQL_BIT,
	    SQL_TINYINT,
	    SQL_SMALLINT,
	    SQL_INTEGER,
	    SQL_BIGINT,
	    SQL_TYPE_DATE,
	    SQL_TYPE_TIME,
	    SQL_TYPE_TIMESTAMP,
	    SQL_DECIMAL,
	    SQL_NUMERIC,
	    SQL_FLOAT,
	    SQL_DOUBLE,
	    SQL_VARCHAR,
	    SQL_VARBINARY,
	    SQL_INTERVAL_YEAR,
	    SQL_INTERVAL_MONTH,
	    SQL_INTERVAL_DAY,
	    SQL_INTERVAL_HOUR,
	    SQL_INTERVAL_MINUTE,
	    SQL_INTERVAL_SECOND,
	    SQL_INTERVAL_YEAR_TO_MONTH,
	    SQL_INTERVAL_DAY_TO_HOUR,
	    SQL_INTERVAL_DAY_TO_MINUTE,
	    SQL_INTERVAL_DAY_TO_SECOND,
	    SQL_INTERVAL_HOUR_TO_MINUTE,
	    SQL_INTERVAL_HOUR_TO_SECOND,
	    SQL_INTERVAL_MINUTE_TO_SECOND,
	};

	while (SQLFetch(hstmt) != SQL_NO_DATA) {
		REQUIRE(data_type == data_types[row_count]);
		row_count++;
	}

	// When type is not supported, SQLGetTypeInfo returns SQL_SUCCESS_WITH_INFO
	EXECUTE_AND_CHECK("SQLBindCol", SQLBindCol, hstmt, 2, SQL_C_SHORT, &data_type, sizeof(data_type), &len_or_ind_ptr);
	EXECUTE_AND_CHECK("SQLGetTypeInfo(SQL_ALL_TYPES)", SQLGetTypeInfo, hstmt, SQL_ALL_TYPES);

	SQLRETURN ret;
	while ((ret = SQLFetch(hstmt)) != SQL_NO_DATA) {
		std::string state, message;
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		ACCESS_DIAGNOSTIC(state, message, hstmt, SQL_HANDLE_STMT);
		REQUIRE(state == "07006");
		REQUIRE(duckdb::StringUtil::Contains(message, "Unsupported type"));
		row_count++;
	}

	// unbind column
	EXECUTE_AND_CHECK("SQLBindCol", SQLBindCol, hstmt, 2, SQL_C_SHORT, nullptr, 0, nullptr);
}

static void TestSQLTables(HSTMT &hstmt, std::map<SQLSMALLINT, SQLULEN> &types_map) {
	SQLRETURN ret;

	EXECUTE_AND_CHECK("SQLTables", SQLTables, hstmt, nullptr, 0, ConvertToSQLCHAR("main"), SQL_NTS,
	                  ConvertToSQLCHAR("%"), SQL_NTS, ConvertToSQLCHAR("TABLE"), SQL_NTS);

	SQLSMALLINT col_count;

	EXECUTE_AND_CHECK("SQLNumResultCols", SQLNumResultCols, hstmt, &col_count);
	REQUIRE(col_count == 5);

	std::vector<MetadataData> expected_metadata = {{"TABLE_CAT", SQL_VARCHAR},
	                                               {"TABLE_SCHEM", SQL_VARCHAR},
	                                               {"TABLE_NAME", SQL_VARCHAR},
	                                               {"TABLE_TYPE", SQL_VARCHAR},
	                                               {"REMARKS", SQL_VARCHAR}};

	for (int i = 0; i < col_count; i++) {
		auto &entry = expected_metadata[i];
		METADATA_CHECK(hstmt, i + 1, entry.col_name.c_str(), entry.col_name.length(), entry.col_type,
		               types_map[entry.col_type], 0, SQL_NULLABLE_UNKNOWN);
	}

	int fetch_count = 0;
	do {
		ret = SQLFetch(hstmt);
		if (ret == SQL_NO_DATA) {
			break;
		}
		if (ret == SQL_SUCCESS_WITH_INFO) {
			std::string state, message;
			ACCESS_DIAGNOSTIC(state, message, hstmt, SQL_HANDLE_STMT);
			REQUIRE(state == "07006");
			REQUIRE(duckdb::StringUtil::Contains(message, "Invalid Input Error"));
		} else {
			ODBC_CHECK(ret, "SQLFetch");
		}
		fetch_count++;

		DATA_CHECK(hstmt, 1, "memory");
		DATA_CHECK(hstmt, 2, "main");

		switch (fetch_count) {
		case 1:
			DATA_CHECK(hstmt, 3, "bool_table");
			break;
		case 2:
			DATA_CHECK(hstmt, 3, "bytea_table");
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

static void TestSQLTablesLong(HSTMT &hstmt) {
	// FIXME: this test is broken
	return;

	EXECUTE_AND_CHECK("SQLTables", SQLTables, hstmt, ConvertToSQLCHAR(""), SQL_NTS, ConvertToSQLCHAR("main"), SQL_NTS,
	                  ConvertToSQLCHAR("test_table_%"), SQL_NTS,
	                  ConvertToSQLCHAR("1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,'TABLE'"),
	                  SQL_NTS);

	DATA_CHECK(hstmt, 1, "memory");
	DATA_CHECK(hstmt, 2, "main");
	DATA_CHECK(hstmt, 3, "test_table_1");
	DATA_CHECK(hstmt, 4, "TABLE");
}

static void TestSQLColumns(HSTMT &hstmt, std::map<SQLSMALLINT, SQLULEN> &types_map) {
	EXECUTE_AND_CHECK("SQLColumns", SQLColumns, hstmt, nullptr, 0, ConvertToSQLCHAR("main"), SQL_NTS,
	                  ConvertToSQLCHAR("%"), SQL_NTS, nullptr, 0);

	SQLSMALLINT col_count;

	EXECUTE_AND_CHECK("SQLNumResultCols", SQLNumResultCols, hstmt, &col_count);
	REQUIRE(col_count == 18);

	// Create a map of column types and a vector of expected metadata
	std::vector<MetadataData> expected_metadata = {
	    {"TABLE_CAT", SQL_INTEGER},         {"TABLE_SCHEM", SQL_VARCHAR},      {"TABLE_NAME", SQL_VARCHAR},
	    {"COLUMN_NAME", SQL_VARCHAR},       {"DATA_TYPE", SQL_BIGINT},         {"TYPE_NAME", SQL_VARCHAR},
	    {"COLUMN_SIZE", SQL_INTEGER},       {"BUFFER_LENGTH", SQL_INTEGER},    {"DECIMAL_DIGITS", SQL_INTEGER},
	    {"NUM_PREC_RADIX", SQL_INTEGER},    {"NULLABLE", SQL_INTEGER},         {"REMARKS", SQL_INTEGER},
	    {"COLUMN_DEF", SQL_VARCHAR},        {"SQL_DATA_TYPE", SQL_BIGINT},     {"SQL_DATETIME_SUB", SQL_BIGINT},
	    {"CHAR_OCTET_LENGTH", SQL_INTEGER}, {"ORDINAL_POSITION", SQL_INTEGER}, {"IS_NULLABLE", SQL_VARCHAR}};

	for (int i = 0; i < col_count; i++) {
		auto &entry = expected_metadata[i];
		METADATA_CHECK(hstmt, i + 1, entry.col_name.c_str(), entry.col_name.length(), entry.col_type,
		               types_map[entry.col_type], 0, SQL_NULLABLE_UNKNOWN);
	}

	std::vector<std::array<std::string, 4>> expected_data = {
	    {"bool_table", "id", "13", "INTEGER"},      {"bool_table", "t", "25", "VARCHAR"},
	    {"bool_table", "b", "10", "BOOLEAN"},       {"bytea_table", "id", "13", "INTEGER"},
	    {"bytea_table", "t", "26", "BLOB"},         {"interval_table", "id", "13", "INTEGER"},
	    {"interval_table", "iv", "27", "INTERVAL"}, {"interval_table", "d", "25", "VARCHAR"},
	    {"lo_test_table", "id", "13", "INTEGER"},   {"lo_test_table", "large_data", "26", "BLOB"},
	    {"test_table_1", "id", "13", "INTEGER"},    {"test_table_1", "t", "25", "VARCHAR"},
	    {"test_view", "id", "13", "INTEGER"},       {"test_view", "t", "25", "VARCHAR"}};

	for (int i = 0; i < expected_data.size(); i++) {
		SQLRETURN ret = SQLFetch(hstmt);
		if (ret == SQL_SUCCESS_WITH_INFO) {
			std::string state, message;
			ACCESS_DIAGNOSTIC(state, message, hstmt, SQL_HANDLE_STMT);
			REQUIRE(state == "07006");
			REQUIRE(duckdb::StringUtil::Contains(message, "Invalid Input Error"));
			ret = SQL_SUCCESS;
		} else {
			ODBC_CHECK(ret, "SQLFetch");
		}

		auto &entry = expected_data[i];
		DATA_CHECK(hstmt, 1, "");
		DATA_CHECK(hstmt, 2, "main");
		DATA_CHECK(hstmt, 3, entry[0]);
		DATA_CHECK(hstmt, 4, entry[1]);
		DATA_CHECK(hstmt, 5, entry[2]);
		DATA_CHECK(hstmt, 6, entry[3]);
	}
}

TEST_CASE("Test Catalog Functions (SQLGetTypeInfo, SQLTables, SQLColumns, SQLGetInfo)", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	auto types_map = InitializeTypesMap();

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Initializes the database with dummy data
	InitializeDatabase(hstmt);

	// Drop the test table if it exists
	EXECUTE_AND_CHECK("SQLExecDirect (DROP TABLE)", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("DROP TABLE IF EXISTS test_table"), SQL_NTS);

	// Check for SQLGetTypeInfo
	TestGetTypeInfo(hstmt, types_map);

	// Check for SQLTables
	TestSQLTables(hstmt, types_map);
	TestSQLTablesLong(hstmt);

	// Check for SQLColumns
	TestSQLColumns(hstmt, types_map);

	// Test SQLGetInfo
	char database_name[128];
	SQLSMALLINT len;
	EXECUTE_AND_CHECK("SQLGetInfo (SQL_TABLE_TERM)", SQLGetInfo, hstmt, SQL_TABLE_TERM, database_name,
	                  sizeof(database_name), &len);
	REQUIRE(STR_EQUAL(database_name, "table"));

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}
