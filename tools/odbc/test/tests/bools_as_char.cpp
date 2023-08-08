#include "../common.h"

using namespace odbc_test;

/**
 * A test to check that bools are correctly converted to char
 */
TEST_CASE("Test bools to char conversion", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	auto types_map = InitializeTypesMap();

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	InitializeDatabase(hstmt);

	/**
	 * A simple query with one text param
	 */

	// Prepare a statement
	EXECUTE_AND_CHECK("SQLPrepare", SQLPrepare, hstmt, ConvertToSQLCHAR("SELECT id, t, b FROM bool_table WHERE t = ?"),
	                  SQL_NTS);

	// Bind param
	const char *param = "yes";
	SQLLEN param_len = SQL_NTS;
	EXECUTE_AND_CHECK("SQLBindParameter", SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 5, 0,
	                  (SQLPOINTER)param, strlen(param), &param_len);

	// Execute
	EXECUTE_AND_CHECK("SQLExecute", SQLExecute, hstmt);

	// Fetch result
	METADATA_CHECK(hstmt, 3, "b", sizeof('b'), SQL_CHAR, types_map[SQL_CHAR], 0, SQL_NULLABLE_UNKNOWN);

	EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, "2");
	DATA_CHECK(hstmt, 2, "yes");
	DATA_CHECK(hstmt, 3, "true");

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);

	/**
	 * A simple query with one boolean param (passed as varchar)
	 */

	// Bind param
	param = "true";
	param_len = SQL_NTS;
	EXECUTE_AND_CHECK("SQLBindParameter", SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 5, 0,
	                  (SQLPOINTER)param, strlen(param), &param_len);

	// Execute
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT id, t, b FROM bool_table WHERE b = ?"), SQL_NTS);

	// Fetch result
	METADATA_CHECK(hstmt, 3, "b", sizeof('b'), SQL_CHAR, types_map[SQL_CHAR], 0, SQL_NULLABLE_UNKNOWN);

	std::vector<std::string> expected_data[3] = {{"1", "yeah", "true"}, {"2", "yes", "true"}, {"3", "true", "true"}};

	for (int i = 0; i < 3; i++) {
		EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
		DATA_CHECK(hstmt, 1, expected_data[i][0]);
		DATA_CHECK(hstmt, 2, expected_data[i][1]);
		DATA_CHECK(hstmt, 3, expected_data[i][2]);
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
