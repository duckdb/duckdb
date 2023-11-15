#include "../common.h"

using namespace odbc_test;

void test_row_column_count(SQLHANDLE stmt) {

	SQLRETURN ret;

	SQLLEN row_count;
	EXECUTE_AND_CHECK("SQLRowCount()", SQLRowCount, stmt, &row_count);
	REQUIRE(row_count == -1);

	SQLSMALLINT col_count;
	EXECUTE_AND_CHECK("SQLNumResultCols()", SQLNumResultCols, stmt, &col_count);
	REQUIRE(col_count == 1);
}

//**************************************************************************
TEST_CASE("Get SQL TIMESTAMP value via SQLGetData", "[odbc][time]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;

	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle()", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &stmt);

	EXECUTE_AND_CHECK("SQLExecDirect()", SQLExecDirect, stmt,
	                  ConvertToSQLCHAR("SELECT TIMESTAMP '1992-09-20 12:34:56.123456' as dt"), SQL_NTS);

	test_row_column_count(stmt);

	SQL_TIMESTAMP_STRUCT ts_val;
	SQLSMALLINT buffer_length = 0;
	unsigned char target_value[4096];
	SQLSMALLINT string_length = 0;
	SQLLEN numeric_attribute;

	EXECUTE_AND_CHECK("SQLColAttribute()", SQLColAttribute, stmt, 1, 2, &target_value, buffer_length, &string_length,
	                  &numeric_attribute);
	REQUIRE(numeric_attribute == SQL_TYPE_TIMESTAMP);

	EXECUTE_AND_CHECK("SQLFetch()", SQLFetch, stmt);

	SQLLEN str_len_or_ind;
	EXECUTE_AND_CHECK("SQLGetData()", SQLGetData, stmt, 1, numeric_attribute, &ts_val, sizeof(ts_val), &str_len_or_ind);

	REQUIRE(ts_val.year == 1992);
	REQUIRE(ts_val.month == 9);
	REQUIRE(ts_val.day == 20);
	REQUIRE(ts_val.hour == 12);
	REQUIRE(ts_val.minute == 34);
	REQUIRE(ts_val.second == 56);
	REQUIRE(ts_val.fraction == 123456);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

//**************************************************************************
TEST_CASE("Get SQL TIMESTAMP value via SQLBindCol", "[odbc][time]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;

	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle()", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &stmt);

	EXECUTE_AND_CHECK("SQLExecDirect()", SQLExecDirect, stmt,
	                  ConvertToSQLCHAR("SELECT TIMESTAMP '1992-09-20 12:34:56.123456' as dt"), SQL_NTS);

	test_row_column_count(stmt);

	SQL_TIMESTAMP_STRUCT ts_val;
	SQLLEN null_val;
	int32_t row = 0;

	EXECUTE_AND_CHECK("SQLBindCol()", SQLBindCol, stmt, 1, SQL_TYPE_TIMESTAMP, &ts_val, row, &null_val);

	EXECUTE_AND_CHECK("SQLFetch()", SQLFetch, stmt);

	REQUIRE(ts_val.year == 1992);
	REQUIRE(ts_val.month == 9);
	REQUIRE(ts_val.day == 20);
	REQUIRE(ts_val.hour == 12);
	REQUIRE(ts_val.minute == 34);
	REQUIRE(ts_val.second == 56);
	REQUIRE(ts_val.fraction == 123456);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

//**************************************************************************
TEST_CASE("Get SQL DATE value via SQLGetData", "[odbc][time]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;

	DATE_STRUCT ts_val;
	SQLSMALLINT buffer_length = 0;
	unsigned char target_value[4096];
	SQLSMALLINT string_length = 0;
	SQLLEN numeric_attribute;
	SQLLEN str_len_or_ind;

	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle()", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &stmt);

	EXECUTE_AND_CHECK("SQLExecDirect()", SQLExecDirect, stmt, ConvertToSQLCHAR("SELECT DATE '1992-09-20' as d"),
	                  SQL_NTS);

	test_row_column_count(stmt);

	EXECUTE_AND_CHECK("SQLColAttribute()", SQLColAttribute, stmt, 1, 2, &target_value, buffer_length, &string_length,
	                  &numeric_attribute);

	REQUIRE(numeric_attribute == SQL_TYPE_DATE);

	EXECUTE_AND_CHECK("SQLFetch()", SQLFetch, stmt);

	EXECUTE_AND_CHECK("SQLGetData()", SQLGetData, stmt, 1, numeric_attribute, &ts_val, sizeof(ts_val), &str_len_or_ind);

	REQUIRE(ts_val.year == 1992);
	REQUIRE(ts_val.month == 9);
	REQUIRE(ts_val.day == 20);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

//**************************************************************************
TEST_CASE("Get SQL DATE value via SQLBindCol", "[odbc][time]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;

	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle()", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &stmt);

	EXECUTE_AND_CHECK("SQLExecDirect()", SQLExecDirect, stmt, ConvertToSQLCHAR("SELECT DATE '1992-09-20' as d"),
	                  SQL_NTS);

	test_row_column_count(stmt);

	DATE_STRUCT ts_val;
	SQLLEN null_val;
	int32_t row = 0;

	EXECUTE_AND_CHECK("SQLBindCol()", SQLBindCol, stmt, 1, SQL_TYPE_DATE, &ts_val, row, &null_val);

	EXECUTE_AND_CHECK("SQLFetch()", SQLFetch, stmt);

	REQUIRE(ts_val.year == 1992);
	REQUIRE(ts_val.month == 9);
	REQUIRE(ts_val.day == 20);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

//**************************************************************************
TEST_CASE("Get SQL TIME value via SQLGetData", "[odbc][time]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;

	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle()", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &stmt);

	EXECUTE_AND_CHECK("SQLExecDirect()", SQLExecDirect, stmt, ConvertToSQLCHAR("SELECT TIME '12:34:56' as t"), SQL_NTS);

	test_row_column_count(stmt);

	TIME_STRUCT ts_val;
	SQLSMALLINT buffer_length = 0;
	unsigned char target_value[4096];
	SQLSMALLINT string_length = 0;
	SQLLEN numeric_attribute;

	EXECUTE_AND_CHECK("SQLColAttribute()", SQLColAttribute, stmt, 1, 2, &target_value, buffer_length, &string_length,
	                  &numeric_attribute);
	REQUIRE(numeric_attribute == SQL_TYPE_TIME);

	EXECUTE_AND_CHECK("SQLFetch()", SQLFetch, stmt);

	SQLLEN str_len_or_ind;
	EXECUTE_AND_CHECK("SQLGetData()", SQLGetData, stmt, 1, numeric_attribute, &ts_val, sizeof(ts_val), &str_len_or_ind);

	REQUIRE(ts_val.hour == 12);
	REQUIRE(ts_val.minute == 34);
	REQUIRE(ts_val.second == 56);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

//**************************************************************************
TEST_CASE("Get SQL TIME value via SQLBindCol", "[odbc][time]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;

	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle()", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &stmt);

	EXECUTE_AND_CHECK("SQLExecDirect()", SQLExecDirect, stmt, ConvertToSQLCHAR("SELECT TIME '12:34:56' as t"), SQL_NTS);

	test_row_column_count(stmt);

	TIME_STRUCT ts_val;
	SQLLEN null_val;
	int32_t row = 0;

	EXECUTE_AND_CHECK("SQLBindCol()", SQLBindCol, stmt, 1, SQL_TYPE_TIME, &ts_val, row, &null_val);

	EXECUTE_AND_CHECK("SQLFetch()", SQLFetch, stmt);

	REQUIRE(ts_val.hour == 12);
	REQUIRE(ts_val.minute == 34);
	REQUIRE(ts_val.second == 56);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
