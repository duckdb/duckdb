#include "../common.h"

using namespace odbc_test;

#define ROW_ARRAY_SIZE 10

struct OrderInfo {
	SQLUINTEGER order_id;
	SQLLEN order_id_ind;
	SQLCHAR sales_person[13];
	SQLLEN sales_person_len_or_ind;
	SQLCHAR status[8];
	SQLLEN status_len_or_ind;
};

// This test is taken from
// https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/row-wise-binding?view=sql-server-ver15
static void TestMicrosoftExample(HSTMT &hstmt) {
	OrderInfo order_info[ROW_ARRAY_SIZE];
	SQLULEN rows_fetched;
	SQLUSMALLINT row_array_status[ROW_ARRAY_SIZE];

	// Specify the size of the structure with the SQL_ATTR_ROW_BIND_TYPE
	// statement attribute. This also declares that row-wise binding will
	// be used. Declare the rowset size with the SQL_ATTR_ROW_ARRAY_SIZE
	// statement attribute. Set the SQL_ATTR_ROW_STATUS_PTR statement
	// attribute to point to the row status array. Set the
	// SQL_ATTR_ROWS_FETCHED_PTR statement attribute to point to
	// NumRowsFetched.
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_BIND_TYPE)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_BIND_TYPE,
	                  reinterpret_cast<SQLPOINTER>(sizeof(order_info)), 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_ARRAY_SIZE)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
	                  reinterpret_cast<SQLPOINTER>(ROW_ARRAY_SIZE), 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_STATUS_PTR)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_STATUS_PTR,
	                  row_array_status, 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROWS_FETCHED_PTR)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROWS_FETCHED_PTR,
	                  &rows_fetched, 0);

	// Bind elements of the first structure in the array to the OrderID,
	// SalesPerson, and Status columns.
	EXECUTE_AND_CHECK("SQLBindCol (OrderID)", SQLBindCol, hstmt, 1, SQL_C_ULONG, &order_info[0].order_id,
	                  0, static_cast<SQLLEN *>(&order_info[0].order_id_ind));
	EXECUTE_AND_CHECK("SQLBindCol (SalesPerson)", SQLBindCol, hstmt, 2, SQL_C_CHAR, order_info[0].sales_person,
	                  sizeof(order_info[0].sales_person),
	                  static_cast<SQLLEN *>(&order_info[0].sales_person_len_or_ind));
	EXECUTE_AND_CHECK("SQLBindCol (Status)", SQLBindCol, hstmt, 3, SQL_C_CHAR, order_info[0].status,
	                  sizeof(order_info[0].status), static_cast<SQLLEN *>(&order_info[0].status_len_or_ind));

	// Execute a statement to retrieve rows from the Orders table.
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT i AS OrderID, i::VARCHAR || 'SalesPerson' AS SalesPerson, i::VARCHAR || 'Status' AS Status FROM range(10) t(i)"), SQL_NTS);

	// Fetch up to the rowset size number of rows at a time. Print the actual
	// number of rows fetched; this number is returned in NumRowsFetched.
	// Check the row status array to print only those rows successfully
	// fetched. Code to check if rc equals SQL_SUCCESS_WITH_INFO or
	// SQL_ERRORnot shown.
	SQLRETURN ret;
	while ((ret = SQLFetchScroll(hstmt, SQL_FETCH_NEXT, 0)) != SQL_NO_DATA) {
		for (int i = 0; i < rows_fetched; i++) {
			if (row_array_status[i] == SQL_ROW_SUCCESS || row_array_status[i] == SQL_ROW_SUCCESS_WITH_INFO) {
				if (order_info[i].order_id_ind == SQL_NULL_DATA) {
					printf("Order ID: NULL\n");
				} else {
					printf("Order ID: %d\n", order_info[i].order_id);
				}
				if (order_info[i].sales_person_len_or_ind == SQL_NULL_DATA) {
					printf("Sales person: NULL\n");
				} else {
					printf("Sales person: %s\n", order_info[i].sales_person);
				}
				if (order_info[i].status_len_or_ind == SQL_NULL_DATA) {
					printf("Status: NULL\n");
				} else {
					printf("Status: %s\n", order_info[i].status);
				}
			}
		}
	}

	EXECUTE_AND_CHECK("SQLCloseCursor", SQLCloseCursor, hstmt);
}

TEST_CASE("Test Row Wise Testing and SQLFetchScroll", "[odbc]") {
	// FIXME: add actual test body
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
