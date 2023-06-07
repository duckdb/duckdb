#include "../common.h"

#include <iostream>

using namespace odbc_test;

#define ROW_ARRAY_SIZE 10

struct OrderInfo {
	SQLUINTEGER order_id;
	SQLULEN order_id_ind;
	SQLCHAR sales_person[13];
	SQLULEN sales_person_len_or_ind;
	SQLCHAR status[8];
	SQLULEN status_len_or_ind;
};

static void TestMicrosoftExample(HSTMT &hstmt) {
	SQLRETURN ret;

	OrderInfo order_info[ROW_ARRAY_SIZE];
	SQLULEN rows_fetched;
	SQLUSMALLINT row_array_status[ROW_ARRAY_SIZE];
	SQLULEN order_info_size = sizeof(order_info);
	ExecuteCmdAndCheckODBC("SQLSetStmtAttr", SQL_HANDLE_STMT, hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
	                       (SQLPOINTER)ROW_ARRAY_SIZE, 0);
}

TEST_CASE("row_wise_fetching", "[odbc") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(ret, env, dbc);

	ExecuteCmdAndCheckODBC("SQLAllocHandle (HSTMT)", SQL_HANDLE_STMT, hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc,
	                       &hstmt);

	// Free the statement handle
	ExecuteCmdAndCheckODBC("SQLFreeHandle (HSTMT)", SQL_HANDLE_STMT, hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(ret, env, dbc);
}
