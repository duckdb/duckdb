#include "../common.h"

using namespace odbc_test;

static void RunDataCheckOnTable(HSTMT &hstmt, int num_rows) {
	for (int i = 1; i <= num_rows; i++) {
		EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
		DATA_CHECK(hstmt, 1, std::to_string(i));
		DATA_CHECK(hstmt, 2, std::string("foo") + std::to_string(i));
	}
}

// Test Simple With Query
static void SimpleWithTest(HSTMT &hstmt) {
	EXECUTE_AND_CHECK("SQLExectDirect(WITH)", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("with recursive cte as (select g, 'foo' || g as foocol from "
	                                   "generate_series(1,10) as g(g)) select * from cte;"),
	                  SQL_NTS);

	RunDataCheckOnTable(hstmt, 10);

	EXECUTE_AND_CHECK("SQLFreeStmt(CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

// Test With Query with Prepare and Execute
static void PreparedWithTest(HSTMT &hstmt) {
	EXECUTE_AND_CHECK("SQLPrepare(WITH)", SQLPrepare, hstmt,
	                  ConvertToSQLCHAR("with cte as (select g, 'foo' || g as foocol from generate_series(1,10) as "
	                                   "g(g)) select * from cte WHERE g < ?"),
	                  SQL_NTS);

	SQLINTEGER param = 3;
	SQLLEN param_len = sizeof(param);
	EXECUTE_AND_CHECK("SQLBindParameter", SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_INTEGER, SQL_INTEGER, 0, 0,
	                  &param, sizeof(param), &param_len);

	EXECUTE_AND_CHECK("SQLExecute", SQLExecute, hstmt);

	RunDataCheckOnTable(hstmt, 2);

	EXECUTE_AND_CHECK("SQLFreeStmt(CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

static void TestCTEandSetFetchEnv(const char *extra_params) {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLDriverConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc, extra_params);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	SimpleWithTest(hstmt);
	PreparedWithTest(hstmt);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

/**
 * Runs two WITH queries both with declare fetch on and off, which should not affect the result
 * because the queries are not using cursors.
 */
TEST_CASE("Test CTE", "[odbc]") {
	// First test with UseDeclareFetch=0
	TestCTEandSetFetchEnv("UseDeclareFetch=0");

	// Then test with UseDeclareFetch=1
	TestCTEandSetFetchEnv("UseDeclareFetch=1;Fetch=1");
}
