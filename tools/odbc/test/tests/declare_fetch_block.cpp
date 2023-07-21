#include "../common.h"

#include <iostream>

using namespace odbc_test;

#define TOTAL 120
#define BLOCK 84

static void TemporaryTableTest(HSTMT &hstmt) {
    EXECUTE_AND_CHECK("SQLExecDirect (CREATE TABLE)", SQLExecDirect, hstmt, ConvertToSQLCHAR("CREATE TEMPORARY TABLE test (id int4 primary key)"), SQL_NTS);

	// Insert 120 rows
	for (int i = 0; i < TOTAL; i++) {
		std::string query = "INSERT INTO test VALUES (" + std::to_string(i) + ")";
		EXECUTE_AND_CHECK("SQLExecDirect (INSERT)", SQLExecDirect, hstmt, ConvertToSQLCHAR(query), SQL_NTS);
	}

	EXECUTE_AND_CHECK("SQLFreeStmt(CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

static void BlockCursorTest(HSTMT &hstmt) {
	SQLULEN rows_fetched;

	EXECUTE_AND_CHECK("SQLSetStmtAttr (ROW_ARRAY_SIZE)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_ARRAY_SIZE, ConvertToSQLPOINTER(BLOCK), 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (ROWS_FETCHED_PTR)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);

	// Bind Column
	SQLINTEGER id[TOTAL];
	SQLLEN id_ind[TOTAL];
	EXECUTE_AND_CHECK("SQLBindCol (id)", SQLBindCol, hstmt, 1, SQL_C_SLONG, id, 0, id_ind);

	// Execute the query
	EXECUTE_AND_CHECK("SQLExecDirect (SELECT)", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT * FROM test"), SQL_NTS);

	// Fetch results
	for (int i = 0; i < 2; i++) {
		EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
		REQUIRE(rows_fetched <= BLOCK);
		for (int j = i * BLOCK; j < i * BLOCK + rows_fetched; j++) {
			REQUIRE(id[j] == j);
		}
	}

	SQLRETURN ret = SQLFetch(hstmt);
	REQUIRE(ret == SQL_NO_DATA);

	EXECUTE_AND_CHECK("SQLFreeStmt(CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

static void ScrollNextTest(HSTMT &hstmt) {

}

TEST_CASE("Test Declare Fetch Block", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLDriverConnect with UseDeclareFetch=1
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "UseDeclareFetch=1");

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	TemporaryTableTest(hstmt);
	BlockCursorTest(hstmt);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}