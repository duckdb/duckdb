#include "../common.h"

using namespace odbc_test;

const int TABLE_SIZE[] = {120, 4096};
const int ARRAY_SIZE[] = {84, 512};

enum ESize { SMALL, LARGE };

static void TemporaryTable(HSTMT &hstmt, ESize S) {
	EXECUTE_AND_CHECK("SQLExecDirect (CREATE TABLE)", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TEMPORARY TABLE test (id int4 primary key)"), SQL_NTS);

	// Insert S size rows
	for (int i = 0; i < TABLE_SIZE[S]; i++) {
		std::string query = "INSERT INTO test VALUES (" + std::to_string(i) + ")";
		EXECUTE_AND_CHECK("SQLExecDirect (INSERT)", SQLExecDirect, hstmt, ConvertToSQLCHAR(query), SQL_NTS);
	}

	EXECUTE_AND_CHECK("SQLFreeStmt(CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

static void BlockCursor(HSTMT &hstmt, ESize S, SQLINTEGER *&id, SQLLEN *&id_ind) {
	SQLULEN rows_fetched;

	// Set array S to ARRAY_SIZE[S]
	EXECUTE_AND_CHECK("SQLSetStmtAttr (ROW_ARRAY_SIZE)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
	                  ConvertToSQLPOINTER(ARRAY_SIZE[S]), 0);
	// Set ROWS_FETCHED_PTR to rows_fetched
	EXECUTE_AND_CHECK("SQLSetStmtAttr (ROWS_FETCHED_PTR)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROWS_FETCHED_PTR,
	                  &rows_fetched, 0);

	// Bind Column
	EXECUTE_AND_CHECK("SQLBindCol (id)", SQLBindCol, hstmt, 1, SQL_C_SLONG, id, 0, id_ind);

	// Execute the query
	EXECUTE_AND_CHECK("SQLExecDirect (SELECT)", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT * FROM test"), SQL_NTS);

	int expected_rows_fetched = 0;
	if (S == SMALL) {
		expected_rows_fetched = 2;
	} else {
		expected_rows_fetched = 8;
	}

	int total_rows_fetched = 0;

	// Fetch results
	for (int i = 0; i < expected_rows_fetched; i++) {
		EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
		REQUIRE(rows_fetched <= ARRAY_SIZE[S]);
		total_rows_fetched += rows_fetched;
		REQUIRE(total_rows_fetched == i * ARRAY_SIZE[S] + rows_fetched);
	}
	REQUIRE(total_rows_fetched == TABLE_SIZE[S]);

	SQLRETURN ret = SQLFetch(hstmt);
	REQUIRE(ret == SQL_NO_DATA);

	EXECUTE_AND_CHECK("SQLFreeStmt(CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

static void FetchRows(HSTMT &hstmt, SQLULEN &rows_fetched, SQLSMALLINT scroll_orientation, ESize S) {
	int total_rows_fetched = 0;
	for (int j = 0; j < TABLE_SIZE[S]; j++) {
		EXECUTE_AND_CHECK("SQLFetchScroll", SQLFetchScroll, hstmt, scroll_orientation, 0);
		REQUIRE(rows_fetched == 1);
		total_rows_fetched++;
	}
	REQUIRE(total_rows_fetched == TABLE_SIZE[S]);
	REQUIRE(SQLFetchScroll(hstmt, scroll_orientation, 0) == SQL_NO_DATA);
}

static void ScrollNext(HSTMT &hstmt, ESize S) {
	SQLULEN rows_fetched;

	// Set array size to 1,
	EXECUTE_AND_CHECK("SQLSetStmtAttr(ROW_ARRAY_SIZE)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
	                  ConvertToSQLPOINTER(1), SQL_IS_INTEGER);
	// Set rows fetched ptr
	EXECUTE_AND_CHECK("SQLSetStmtAttr (ROWS_FETCHED_PTR)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROWS_FETCHED_PTR,
	                  &rows_fetched, 0);
	// Cursor Type to Static: which means data in the result set is static
	EXECUTE_AND_CHECK("SQLSetStmtAttr(CURSOR_TYPE)", SQLSetStmtAttr, hstmt, SQL_ATTR_CURSOR_TYPE,
	                  ConvertToSQLPOINTER(SQL_CURSOR_STATIC), 0);
	// and Concurrency to Rowver: Cursor uses optimistic concurrency control, comparing row versions such as SQLBase
	// ROWID or Sybase TIMESTAMP.
	SQLRETURN ret = SQLSetStmtAttr(hstmt, SQL_ATTR_CONCURRENCY, ConvertToSQLPOINTER(SQL_CONCUR_ROWVER), 0);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);

	// Execute the query
	EXECUTE_AND_CHECK("SQLExecDirect (SELECT)", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT * FROM test"), SQL_NTS);

	// Fetch results using SQLFetchScroll
	for (int i = 0; i < 2; i++) {
		// First check if fetch next works
		FetchRows(hstmt, rows_fetched, SQL_FETCH_NEXT, S);

		// Then check if fetch prior works
		FetchRows(hstmt, rows_fetched, SQL_FETCH_PRIOR, S);
	}

	// Close the cursor
	EXECUTE_AND_CHECK("SQLFreeStmt(CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
	// Unbind the columns
	EXECUTE_AND_CHECK("SQLFreeStmt(UNBIND)", SQLFreeStmt, hstmt, SQL_UNBIND);
}

static void FetchAbsolute(HSTMT &hstmt, ESize S) {
	SQLULEN rows_fetched;
	// Set rows fetched ptr
	EXECUTE_AND_CHECK("SQLSetStmtAttr (ROWS_FETCHED_PTR)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROWS_FETCHED_PTR,
	                  &rows_fetched, 0);

	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT * FROM test"), SQL_NTS);

	// Fetch beyond the last row, should return SQL_NO_DATA
	SQLRETURN ret = SQLFetchScroll(hstmt, SQL_FETCH_ABSOLUTE, TABLE_SIZE[S] + 1);
	REQUIRE(ret == SQL_NO_DATA);

	EXECUTE_AND_CHECK("SQLFetchScroll (ABSOLUTE, 1)", SQLFetchScroll, hstmt, SQL_FETCH_ABSOLUTE, 1);

	// Keep fetching until we reach the last row
	int id;
	for (id = 1; id; id++) {
		if (SQLFetchScroll(hstmt, SQL_FETCH_NEXT, 0) == SQL_NO_DATA) {
			id--;
			break;
		}
	}
	REQUIRE(id == TABLE_SIZE[S]);

	// Close the cursor
	EXECUTE_AND_CHECK("SQLFreeStmt(CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

TEST_CASE("Test Using SQLFetchScroll with different orrientations", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	// Perform the tests for both SMALL and LARGE tables and different fetch sizes
	ESize size[] = {SMALL, LARGE};
	for (int i = 0; i < 2; i++) {
		// Connect to the database using SQLDriverConnect with UseDeclareFetch=1
		DRIVER_CONNECT_TO_DATABASE(env, dbc, "UseDeclareFetch=1");

		// Allocate a statement handle
		EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

		// Create a temporary table and insert size[i] rows
		TemporaryTable(hstmt, size[i]);

		SQLINTEGER *id = new SQLINTEGER[TABLE_SIZE[size[i]]];
		SQLLEN *id_ind = new SQLLEN[TABLE_SIZE[size[i]]];
		// Block cursor, fetch rows in blocks of size[i]
		BlockCursor(hstmt, size[i], id, id_ind);

		// Scroll cursor, fetch rows one by one
		ScrollNext(hstmt, size[i]);

		// Fetch rows using SQL_FETCH_ABSOLUTE
		FetchAbsolute(hstmt, size[i]);

		delete[] id;
		delete[] id_ind;

		// Free the statement handle
		EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
		EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

		DISCONNECT_FROM_DATABASE(env, dbc);
	}
}
