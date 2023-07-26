#include "../common.h"

using namespace odbc_test;

/**
 * Bind a parameter and execute a query. Check that the result is as expected.
 */
static void BindParamAndExecute(HSTMT &hstmt, SQLCHAR *query, SQLCHAR *param,
                                const std::vector<std::string> &expected_result) {
	SQLLEN len = strlen((char *)param);

	EXECUTE_AND_CHECK("SQLBindParameter (param)", SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 20,
	                  0, param, len, &len);

	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt, query, SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
	for (int i = 0; i < expected_result.size(); i++) {
		DATA_CHECK(hstmt, i + 1, expected_result[i]);
	}

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

TEST_CASE("Test parameter quoting and in combination with special characters", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Check that the driver escapes quotes correctly
	BindParamAndExecute(hstmt, ConvertToSQLCHAR("SELECT 'foo', ?::text"), ConvertToSQLCHAR("param'quote"),
	                    {"foo", "param'quote"});
	BindParamAndExecute(hstmt, ConvertToSQLCHAR("SELECT 'foo', ?::text"), ConvertToSQLCHAR("param\\backlash"),
	                    {"foo", "param\\backlash"});
	BindParamAndExecute(hstmt, ConvertToSQLCHAR("SELECT 'foo', ?::text"), ConvertToSQLCHAR("ends with backslash\\"),
	                    {"foo", "ends with backslash\\"});

	// Check that the driver's build-in parser interprets quotes correctly. Check that it distinguishes between ?
	// parameter markers and ? literals.
	BindParamAndExecute(hstmt, ConvertToSQLCHAR("SELECT 'doubled '' quotes', ?::text"), ConvertToSQLCHAR("param"),
	                    {"doubled ' quotes", "param"});
	BindParamAndExecute(hstmt, ConvertToSQLCHAR("SELECT E'escaped quote\\' here', ?::text"), ConvertToSQLCHAR("param"),
	                    {"escaped quote' here", "param"});
	BindParamAndExecute(hstmt, ConvertToSQLCHAR("SELECT $$dollar quoted string$$, ?::text"), ConvertToSQLCHAR("param"),
	                    {"dollar quoted string", "param"});
	BindParamAndExecute(hstmt, ConvertToSQLCHAR("SELECT $xx$complex $dollar quotes$xx$, ?::text"),
	                    ConvertToSQLCHAR("param"), {"complex $dollar quotes", "param"});
	BindParamAndExecute(hstmt, ConvertToSQLCHAR("SELECT $dollar$more complex $dollar quotes$dollar$, ?::text"),
	                    ConvertToSQLCHAR("param"), {"more complex $dollar quotes", "param"});

	// Test backlash escaping without the E'' syntax
	BindParamAndExecute(hstmt, ConvertToSQLCHAR("SELECT 'escaped quote'' here', ?::text"), ConvertToSQLCHAR("param"),
	                    {"escaped quote' here", "param"});
	BindParamAndExecute(hstmt, ConvertToSQLCHAR("SELECT ?::text, '1' a$1"), ConvertToSQLCHAR("$ in an identifier"),
	                    {"$ in an identifier", "1"});
	BindParamAndExecute(hstmt, ConvertToSQLCHAR("SELECT '1'::text a$$S1,?::text,$$2 $'s in an identifier$$::text"),
	                    ConvertToSQLCHAR("param"), {"1", "param", "2 $'s in an identifier"});

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
