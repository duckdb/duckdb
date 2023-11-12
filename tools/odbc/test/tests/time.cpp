#include "../common.h"

using namespace odbc_test;

//**************************************************************************
TEST_CASE("timestamp_SQLGetData", "[odbc][time]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	auto dsn = "Driver=DuckDB Driver;Database=test.duckdb;";

	ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	ODBC_CHECK(ret, "SQLAllocHandle(env)");

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
	ODBC_CHECK(ret, "SQLSetEnvAttr");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	ODBC_CHECK(ret, "SQLAllocHandle(dbc)");

	ret = SQLConnect(dbc, ConvertToSQLCHAR(dsn), SQL_NTS, NULL, 0, NULL, 0);
	ODBC_CHECK(ret, "SQLConnect");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	ODBC_CHECK(ret, "SQLAllocHandle(stmt)");

	ret = SQLExecDirect(stmt, (SQLCHAR *)"SELECT TIMESTAMP '1992-09-20 11:30:00.123456' as dt", SQL_NTS);
	ODBC_CHECK(ret, "SELECT TIMESTAMP '1992-09-20 11:30:00.123456' as dt");

	SQLLEN row_count;
	ret = SQLRowCount(stmt, &row_count);
	ODBC_CHECK(ret, "SQLRowCount(stmt)");
	REQUIRE(row_count == -1);

	SQLSMALLINT col_count;
	ret = SQLNumResultCols(stmt, &col_count);
	ODBC_CHECK(ret, "SQLNumResultCols(stmt)");
	REQUIRE(col_count == 1);

	SQL_TIMESTAMP_STRUCT ts_val;
	SQLSMALLINT buffer_length = 0;
	unsigned char target_value[4096];
	SQLSMALLINT string_length = 0;
	SQLLEN numeric_attribute;

	ret = SQLColAttribute(stmt, 1, 2, &target_value, buffer_length, &string_length, &numeric_attribute);
	ODBC_CHECK(ret, "SQLColAttributeW(stmt)");
	REQUIRE(numeric_attribute == SQL_TYPE_TIMESTAMP);

	ret = SQLFetch(stmt);
	ODBC_CHECK(ret, "SQLFetch");

	SQLLEN str_len_or_ind;
	ret = SQLGetData(stmt, 1, numeric_attribute, &ts_val, sizeof(ts_val), &str_len_or_ind);
	ODBC_CHECK(ret, "SQLGetData(stmt)");

	REQUIRE(ts_val.year == 1992);
	REQUIRE(ts_val.month == 9);
	REQUIRE(ts_val.day == 20);
	REQUIRE(ts_val.hour == 11);
	REQUIRE(ts_val.minute == 30);
	REQUIRE(ts_val.second == 0);
	REQUIRE(ts_val.fraction == 123456);

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	ODBC_CHECK(ret, "SQLFreeHandle(stmt)");

	ret = SQLDisconnect(dbc);
	ODBC_CHECK(ret, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	ODBC_CHECK(ret, "SQLFreeHandle(dbc)");

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	ODBC_CHECK(ret, "SQLFreeHandle(env)");
}

//**************************************************************************
TEST_CASE("timestamp_SQLBindCol", "[odbc][time]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	auto dsn = "Driver=DuckDB Driver;Database=test.duckdb;";

	ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	ODBC_CHECK(ret, "SQLAllocHandle(env)");

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
	ODBC_CHECK(ret, "SQLSetEnvAttr");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	ODBC_CHECK(ret, "SQLAllocHandle(dbc)");

	ret = SQLConnect(dbc, ConvertToSQLCHAR(dsn), SQL_NTS, NULL, 0, NULL, 0);
	ODBC_CHECK(ret, "SQLConnect");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	ODBC_CHECK(ret, "SQLAllocHandle(stmt)");

	ret = SQLExecDirect(stmt, (SQLCHAR *)"SELECT TIMESTAMP '1992-09-20 11:30:00.123456' as dt", SQL_NTS);
	ODBC_CHECK(ret, "SELECT TIMESTAMP '1992-09-20 11:30:00.123456' as dt");

	SQLLEN row_count;
	ret = SQLRowCount(stmt, &row_count);
	ODBC_CHECK(ret, "SQLRowCount(stmt)");
	REQUIRE(row_count == -1);

	SQLSMALLINT col_count;
	ret = SQLNumResultCols(stmt, &col_count);
	ODBC_CHECK(ret, "SQLNumResultCols(stmt)");
	REQUIRE(col_count == 1);

	SQL_TIMESTAMP_STRUCT ts_val;
	SQLLEN null_val;
	int32_t row = 0;

	ret = SQLBindCol(stmt, 1, SQL_TYPE_TIMESTAMP, &ts_val, row, &null_val);
	ODBC_CHECK(ret, "SQLBindCol");

	ret = SQLFetch(stmt);
	ODBC_CHECK(ret, "SQLFetch");

	REQUIRE(ts_val.year == 1992);
	REQUIRE(ts_val.month == 9);
	REQUIRE(ts_val.day == 20);
	REQUIRE(ts_val.hour == 11);
	REQUIRE(ts_val.minute == 30);
	REQUIRE(ts_val.second == 0);
	REQUIRE(ts_val.fraction == 123456);

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	ODBC_CHECK(ret, "SQLFreeHandle(stmt)");

	ret = SQLDisconnect(dbc);
	ODBC_CHECK(ret, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	ODBC_CHECK(ret, "SQLFreeHandle(dbc)");

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	ODBC_CHECK(ret, "SQLFreeHandle(env)");
}

//**************************************************************************
TEST_CASE("date_SQLGetData", "[odbc][time]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	auto dsn = "Driver=DuckDB Driver;Database=test.duckdb;";

	ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	ODBC_CHECK(ret, "SQLAllocHandle(env)");

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
	ODBC_CHECK(ret, "SQLSetEnvAttr");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	ODBC_CHECK(ret, "SQLAllocHandle(dbc)");

	ret = SQLConnect(dbc, ConvertToSQLCHAR(dsn), SQL_NTS, NULL, 0, NULL, 0);
	ODBC_CHECK(ret, "SQLConnect");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	ODBC_CHECK(ret, "SQLAllocHandle(stmt)");

	ret = SQLExecDirect(stmt, (SQLCHAR *)"SELECT DATE '1992-09-20' as d", SQL_NTS);
	ODBC_CHECK(ret, "SELECT DATE '1992-09-20' as d");

	SQLLEN row_count;
	ret = SQLRowCount(stmt, &row_count);
	ODBC_CHECK(ret, "SQLRowCount(stmt)");
	REQUIRE(row_count == -1);

	SQLSMALLINT col_count;
	ret = SQLNumResultCols(stmt, &col_count);
	ODBC_CHECK(ret, "SQLNumResultCols(stmt)");
	REQUIRE(col_count == 1);

	DATE_STRUCT ts_val;
	SQLSMALLINT buffer_length = 0;
	unsigned char target_value[4096];
	SQLSMALLINT string_length = 0;
	SQLLEN numeric_attribute;

	ret = SQLColAttribute(stmt, 1, 2, &target_value, buffer_length, &string_length, &numeric_attribute);
	ODBC_CHECK(ret, "SQLColAttributeW(stmt)");
	REQUIRE(numeric_attribute == SQL_TYPE_DATE);

	ret = SQLFetch(stmt);
	ODBC_CHECK(ret, "SQLFetch");

	SQLLEN str_len_or_ind;
	ret = SQLGetData(stmt, 1, numeric_attribute, &ts_val, sizeof(ts_val), &str_len_or_ind);
	ODBC_CHECK(ret, "SQLGetData(stmt)");

	REQUIRE(ts_val.year == 1992);
	REQUIRE(ts_val.month == 9);
	REQUIRE(ts_val.day == 20);

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	ODBC_CHECK(ret, "SQLFreeHandle(stmt)");

	ret = SQLDisconnect(dbc);
	ODBC_CHECK(ret, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	ODBC_CHECK(ret, "SQLFreeHandle(dbc)");

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	ODBC_CHECK(ret, "SQLFreeHandle(env)");
}

//**************************************************************************
TEST_CASE("date_SQLBindCol", "[odbc][time]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	auto dsn = "Driver=DuckDB Driver;Database=test.duckdb;";

	ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	ODBC_CHECK(ret, "SQLAllocHandle(env)");

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
	ODBC_CHECK(ret, "SQLSetEnvAttr");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	ODBC_CHECK(ret, "SQLAllocHandle(dbc)");

	ret = SQLConnect(dbc, ConvertToSQLCHAR(dsn), SQL_NTS, NULL, 0, NULL, 0);
	ODBC_CHECK(ret, "SQLConnect");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	ODBC_CHECK(ret, "SQLAllocHandle(stmt)");

	ret = SQLExecDirect(stmt, (SQLCHAR *)"SELECT DATE '1992-09-20' as d", SQL_NTS);
	ODBC_CHECK(ret, "SELECT DATE '1992-09-20' as d");

	SQLLEN row_count;
	ret = SQLRowCount(stmt, &row_count);
	ODBC_CHECK(ret, "SQLRowCount(stmt)");
	REQUIRE(row_count == -1);

	SQLSMALLINT col_count;
	ret = SQLNumResultCols(stmt, &col_count);
	ODBC_CHECK(ret, "SQLNumResultCols(stmt)");
	REQUIRE(col_count == 1);

	DATE_STRUCT ts_val;
	SQLLEN null_val;
	int32_t row = 0;

	ret = SQLBindCol(stmt, 1, SQL_TYPE_DATE, &ts_val, row, &null_val);
	ODBC_CHECK(ret, "SQLBindCol");

	ret = SQLFetch(stmt);
	ODBC_CHECK(ret, "SQLFetch");

	REQUIRE(ts_val.year == 1992);
	REQUIRE(ts_val.month == 9);
	REQUIRE(ts_val.day == 20);

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	ODBC_CHECK(ret, "SQLFreeHandle(stmt)");

	ret = SQLDisconnect(dbc);
	ODBC_CHECK(ret, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	ODBC_CHECK(ret, "SQLFreeHandle(dbc)");

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	ODBC_CHECK(ret, "SQLFreeHandle(env)");
}

//**************************************************************************
TEST_CASE("time_SQLGetData", "[odbc][time]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	auto dsn = "Driver=DuckDB Driver;Database=test.duckdb;";

	ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	ODBC_CHECK(ret, "SQLAllocHandle(env)");

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
	ODBC_CHECK(ret, "SQLSetEnvAttr");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	ODBC_CHECK(ret, "SQLAllocHandle(dbc)");

	ret = SQLConnect(dbc, ConvertToSQLCHAR(dsn), SQL_NTS, NULL, 0, NULL, 0);
	ODBC_CHECK(ret, "SQLConnect");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	ODBC_CHECK(ret, "SQLAllocHandle(stmt)");

	ret = SQLExecDirect(stmt, (SQLCHAR *)"SELECT TIME '12:34:56' as t", SQL_NTS);
	ODBC_CHECK(ret, "SELECT TIME '12:34:56' as t");

	SQLLEN row_count;
	ret = SQLRowCount(stmt, &row_count);
	ODBC_CHECK(ret, "SQLRowCount(stmt)");
	REQUIRE(row_count == -1);

	SQLSMALLINT col_count;
	ret = SQLNumResultCols(stmt, &col_count);
	ODBC_CHECK(ret, "SQLNumResultCols(stmt)");
	REQUIRE(col_count == 1);

	TIME_STRUCT ts_val;
	SQLSMALLINT buffer_length = 0;
	unsigned char target_value[4096];
	SQLSMALLINT string_length = 0;
	SQLLEN numeric_attribute;

	ret = SQLColAttribute(stmt, 1, 2, &target_value, buffer_length, &string_length, &numeric_attribute);
	ODBC_CHECK(ret, "SQLColAttributeW(stmt)");
	REQUIRE(numeric_attribute == SQL_TYPE_TIME);

	ret = SQLFetch(stmt);
	ODBC_CHECK(ret, "SQLFetch");

	SQLLEN str_len_or_ind;
	ret = SQLGetData(stmt, 1, numeric_attribute, &ts_val, sizeof(ts_val), &str_len_or_ind);
	ODBC_CHECK(ret, "SQLGetData(stmt)");

	REQUIRE(ts_val.hour == 12);
	REQUIRE(ts_val.minute == 34);
	REQUIRE(ts_val.second == 56);

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	ODBC_CHECK(ret, "SQLFreeHandle(stmt)");

	ret = SQLDisconnect(dbc);
	ODBC_CHECK(ret, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	ODBC_CHECK(ret, "SQLFreeHandle(dbc)");

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	ODBC_CHECK(ret, "SQLFreeHandle(env)");
}

//**************************************************************************
TEST_CASE("time_SQLBindCol", "[odbc][time]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	auto dsn = "Driver=DuckDB Driver;Database=test.duckdb;";

	ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	ODBC_CHECK(ret, "SQLAllocHandle(env)");

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
	ODBC_CHECK(ret, "SQLSetEnvAttr");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	ODBC_CHECK(ret, "SQLAllocHandle(dbc)");

	ret = SQLConnect(dbc, ConvertToSQLCHAR(dsn), SQL_NTS, NULL, 0, NULL, 0);
	ODBC_CHECK(ret, "SQLConnect");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	ODBC_CHECK(ret, "SQLAllocHandle(stmt)");

	ret = SQLExecDirect(stmt, (SQLCHAR *)"SELECT TIME '12:34:56' as t", SQL_NTS);
	ODBC_CHECK(ret, "SELECT TIME '12:34:56' as t");

	SQLLEN row_count;
	ret = SQLRowCount(stmt, &row_count);
	ODBC_CHECK(ret, "SQLRowCount(stmt)");
	REQUIRE(row_count == -1);

	SQLSMALLINT col_count;
	ret = SQLNumResultCols(stmt, &col_count);
	ODBC_CHECK(ret, "SQLNumResultCols(stmt)");
	REQUIRE(col_count == 1);

	TIME_STRUCT ts_val;
	SQLLEN null_val;
	int32_t row = 0;

	ret = SQLBindCol(stmt, 1, SQL_TYPE_TIME, &ts_val, row, &null_val);
	ODBC_CHECK(ret, "SQLBindCol");

	ret = SQLFetch(stmt);
	ODBC_CHECK(ret, "SQLFetch");

	REQUIRE(ts_val.hour == 12);
	REQUIRE(ts_val.minute == 34);
	REQUIRE(ts_val.second == 56);

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	ODBC_CHECK(ret, "SQLFreeHandle(stmt)");

	ret = SQLDisconnect(dbc);
	ODBC_CHECK(ret, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	ODBC_CHECK(ret, "SQLFreeHandle(dbc)");

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	ODBC_CHECK(ret, "SQLFreeHandle(env)");
}



