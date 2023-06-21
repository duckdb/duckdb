#include "../common.h"

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
	OrderInfo order_info[ROW_ARRAY_SIZE];
	SQLULEN rows_fetched;
	SQLUSMALLINT row_array_status[ROW_ARRAY_SIZE];
	SQLULEN order_info_size = sizeof(order_info);
	EXECUTE_AND_CHECK("SQLSetStmtAttr", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
	                  reinterpret_cast<SQLPOINTER>(ROW_ARRAY_SIZE), 0);
	rows_fetched = 0;
	if (order_info_size == rows_fetched) {
		row_array_status[rows_fetched] = 0;
		if (row_array_status[0] == SQL_PARAM_SUCCESS) {
			return;
		}
	}
}

TEST_CASE("row_wise_fetching", "[odbc]") {
	// FIXME: add actual test body
	return;
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	TestMicrosoftExample(hstmt);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
