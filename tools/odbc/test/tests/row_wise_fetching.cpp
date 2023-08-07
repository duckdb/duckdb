#include "../common.h"

using namespace odbc_test;

#define ROW_ARRAY_SIZE 10

typedef struct s_order_info {
	SQLUINTEGER order_id;
	SQLLEN order_id_ind;
	SQLCHAR sales_person[13];
	SQLLEN sales_person_len_or_ind;
	SQLCHAR status[8];
	SQLLEN status_len_or_ind;
} t_order_info;

// This test is taken from
// https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/row-wise-binding?view=sql-server-ver15
static void TestRowWiseExampleTable(HSTMT &hstmt) {
	t_order_info order_info[ROW_ARRAY_SIZE];
	SQLULEN rows_fetched;
	SQLUSMALLINT row_array_status[ROW_ARRAY_SIZE];

	// Specify the size of the structure with the SQL_ATTR_ROW_BIND_TYPE
	// statement attribute. This also declares that row-wise binding will
	// be used. Declare the row set size with the SQL_ATTR_ROW_ARRAY_SIZE
	// statement attribute. Set the SQL_ATTR_ROW_STATUS_PTR statement
	// attribute to point to the row status array. Set the
	// SQL_ATTR_ROWS_FETCHED_PTR statement attribute to point to
	// NumRowsFetched.

	SQLULEN row_bind_type = sizeof(t_order_info);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_BIND_TYPE)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_BIND_TYPE,
	                  reinterpret_cast<SQLPOINTER>(row_bind_type), 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_ARRAY_SIZE)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
	                  reinterpret_cast<SQLPOINTER>(ROW_ARRAY_SIZE), 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_STATUS_PTR)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_STATUS_PTR,
	                  row_array_status, 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROWS_FETCHED_PTR)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROWS_FETCHED_PTR,
	                  &rows_fetched, 0);

	// Bind elements of the first structure in the array to the OrderID,
	// SalesPerson, and Status columns.
	EXECUTE_AND_CHECK("SQLBindCol (OrderID)", SQLBindCol, hstmt, 1, SQL_C_ULONG, &order_info[0].order_id, 0,
	                  &order_info[0].order_id_ind);
	EXECUTE_AND_CHECK("SQLBindCol (SalesPerson)", SQLBindCol, hstmt, 2, SQL_C_CHAR, order_info[0].sales_person,
	                  sizeof(order_info[0].sales_person), &order_info[0].sales_person_len_or_ind);
	EXECUTE_AND_CHECK("SQLBindCol (Status)", SQLBindCol, hstmt, 3, SQL_C_CHAR, order_info[0].status,
	                  sizeof(order_info[0].status), &order_info[0].status_len_or_ind);

	// Execute a statement to retrieve rows from the Orders table.
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT i AS OrderID, i::VARCHAR || 'SalesPerson' AS SalesPerson, i::VARCHAR || "
	                                   "'Status' AS Status FROM range(10) t(i)"),
	                  SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetchScroll", SQLFetchScroll, hstmt, SQL_FETCH_NEXT, 0);
	REQUIRE(rows_fetched == ROW_ARRAY_SIZE);
	for (int i = 0; i < rows_fetched; i++) {
		if (row_array_status[i] == SQL_ROW_SUCCESS || row_array_status[i] == SQL_ROW_SUCCESS_WITH_INFO) {
			REQUIRE(order_info[i].order_id_ind != SQL_NO_DATA);
			REQUIRE(order_info[i].order_id == i);
			REQUIRE(order_info[i].sales_person_len_or_ind != SQL_NO_DATA);
			REQUIRE(ConvertToString(order_info[i].sales_person) == (std::to_string(i) + "SalesPerson"));
			REQUIRE(order_info[i].status_len_or_ind != SQL_NO_DATA);
			REQUIRE(ConvertToString(order_info[i].status) == (std::to_string(i) + "Status"));
		}
	}

	EXECUTE_AND_CHECK("SQLCloseCursor", SQLCloseCursor, hstmt);
}

typedef struct s_many_sql_types {
	SQLCHAR b[2];            // boolean (2 chars because of \0)
	SQLLEN b_ind;            // boolean indicator
	SQLUINTEGER u_i;         // unsigned integer
	SQLLEN u_i_ind;          // unsigned integer indicator
	SQLINTEGER i;            // integer
	SQLLEN i_ind;            // integer indicator
	SQLDOUBLE d;             // double
	SQLLEN d_ind;            // double indicator
	SQL_NUMERIC_STRUCT n;    // numeric
	SQLLEN n_ind;            // numeric indicator
	SQLCHAR vchar[16];       // varchar (16 chars)
	SQLLEN vchar_len_or_ind; // varchar len or indicator
	SQLDATE date[10];        // data (10 chars)
	SQLLEN date_len_or_ind;  // date len or indicator
} t_many_sql_types;

void TestManySQLTypes(HSTMT &hstmt) {
	t_many_sql_types many_sql_types[ROW_ARRAY_SIZE];
	SQLULEN rows_fetched;
	SQLUSMALLINT row_array_status[ROW_ARRAY_SIZE];

	auto row_size = sizeof(t_many_sql_types);

	// SQLSetStmtAttr is used to set attributes that govern the behavior of a statement.

	// Specify the size of the structure with the SQL_ATTR_ROW_BIND_TYPE
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_BIND_TYPE)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_BIND_TYPE,
	                  reinterpret_cast<SQLPOINTER>(row_size), 0);
	// Specify the number of rows to be fetched with the SQL_ATTR_ROW_ARRAY_SIZE
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_ARRAY_SIZE)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
	                  reinterpret_cast<SQLPOINTER>(ROW_ARRAY_SIZE), 0);
	// Specify the address of the array of row status pointers with the SQL_ATTR_ROW_STATUS_PTR
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_STATUS_PTR)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_STATUS_PTR,
	                  row_array_status, 0);
	// Specify the address of the variable that will receive the number of rows fetched with the
	// SQL_ATTR_ROWS_FETCHED_PTR
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROWS_FETCHED_PTR)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROWS_FETCHED_PTR,
	                  &rows_fetched, 0);

	// Bind elements of the first structure to the array
	EXECUTE_AND_CHECK("SQLBindCol (b)", SQLBindCol, hstmt, 1, SQL_C_CHAR, many_sql_types[0].b,
	                  sizeof(many_sql_types[0].b), &many_sql_types[0].b_ind);
	EXECUTE_AND_CHECK("SQLBindCol (u_i)", SQLBindCol, hstmt, 2, SQL_C_ULONG, &many_sql_types[0].u_i,
	                  sizeof(many_sql_types[0].u_i), &many_sql_types[0].u_i_ind);
	EXECUTE_AND_CHECK("SQLBindCol (i)", SQLBindCol, hstmt, 3, SQL_C_LONG, &many_sql_types[0].i,
	                  sizeof(many_sql_types[0].i), &many_sql_types[0].i_ind);
	EXECUTE_AND_CHECK("SQLBindCol (d)", SQLBindCol, hstmt, 4, SQL_C_DOUBLE, &many_sql_types[0].d,
	                  sizeof(many_sql_types[0].d), &many_sql_types[0].d_ind);
	EXECUTE_AND_CHECK("SQLBindCol (n)", SQLBindCol, hstmt, 5, SQL_C_NUMERIC, &many_sql_types[0].n,
	                  sizeof(many_sql_types[0].n), &many_sql_types[0].n_ind);
	EXECUTE_AND_CHECK("SQLBindCol (vchar)", SQLBindCol, hstmt, 6, SQL_C_CHAR, many_sql_types[0].vchar,
	                  sizeof(many_sql_types[0].vchar), &many_sql_types[0].vchar_len_or_ind);
	EXECUTE_AND_CHECK("SQLBindCol (date)", SQLBindCol, hstmt, 7, SQL_C_TYPE_DATE, many_sql_types[0].date,
	                  sizeof(many_sql_types[0].date), &many_sql_types[0].date_len_or_ind);

	EXECUTE_AND_CHECK(
	    "SQLExecDirect", SQLExecDirect, hstmt,
	    ConvertToSQLCHAR("SELECT i::bool::char, i::uint8, i::int8, i::double, i::numeric, i::varchar || '-Varchar',"
	                     "('200' || i::char || '-10-1' || i::char )::date FROM range(10) t(i)"),
	    SQL_NTS);

	// Fetch the data using SQLFetchScroll which fetches the next rowset of data from the result set.
	EXECUTE_AND_CHECK("SQLFetchScroll", SQLFetchScroll, hstmt, SQL_FETCH_NEXT, 0);
	REQUIRE(rows_fetched == ROW_ARRAY_SIZE);

	for (int i = 0; i < ROW_ARRAY_SIZE; i++) {
		REQUIRE(many_sql_types[i].b_ind != SQL_NO_DATA);
		REQUIRE(ConvertToString(many_sql_types[i].b) == (i ? "t" : "f"));
		REQUIRE(many_sql_types[i].u_i_ind != SQL_NO_DATA);
		REQUIRE(many_sql_types[i].u_i == i);
		REQUIRE(many_sql_types[i].i_ind != SQL_NO_DATA);
		REQUIRE(many_sql_types[i].i == i);
		REQUIRE(many_sql_types[i].d_ind != SQL_NO_DATA);
		REQUIRE(many_sql_types[i].d == i);
		REQUIRE(many_sql_types[i].n_ind != SQL_NO_DATA);
		REQUIRE(many_sql_types[i].n.val[0] == i);
		REQUIRE(many_sql_types[i].vchar_len_or_ind != SQL_NO_DATA);
		REQUIRE(ConvertToString(many_sql_types[i].vchar) == (std::to_string(i) + "-Varchar"));
		REQUIRE(many_sql_types[i].date_len_or_ind != SQL_NO_DATA);

		DATE_STRUCT *date = (DATE_STRUCT *)many_sql_types[i].date;
		REQUIRE(date->year == 2000 + i);
		REQUIRE(date->day == 10 + i);
	}

	EXECUTE_AND_CHECK("SQLCloseCursor", SQLCloseCursor, hstmt);
}

TEST_CASE("Test Row Wise Testing and SQLFetchScroll", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Test taken from ODBC documentation
	TestRowWiseExampleTable(hstmt);

	TestManySQLTypes(hstmt);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
