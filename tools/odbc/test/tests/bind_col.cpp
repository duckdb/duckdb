#include <array>
#include "../common.h"

using namespace odbc_test;

TEST_CASE("Test SQLBindCol (binding columns to application buffers)", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;
	SQLINTEGER long_value;
	SQLLEN ind_long_value;
	char char_value[100];
	SQLLEN ind_char_value;

	// Connect to the database
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Bind the first column (long_value) to a long
	EXECUTE_AND_CHECK("SQLBindCol (HSTMT)", SQLBindCol, hstmt, 1, SQL_C_LONG, &long_value, sizeof(SQLINTEGER),
	                  &ind_long_value);

	// Bind the second column (char_value) to a string
	EXECUTE_AND_CHECK("SQLBindCol (HSTMT)", SQLBindCol, hstmt, 2, SQL_C_CHAR, &char_value, sizeof(char_value),
	                  &ind_char_value);

	// Execute the query
	EXECUTE_AND_CHECK("SQLExecDirect (HSTMT)", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT id, 'foo' || id FROM generate_series(1, 10) id(id)"), SQL_NTS);

	SQLINTEGER row_num = 0;
	std::array<int, 10> id = {1, 2, 3, 4, 5, 6, 7, 7, 7, 7};
	std::array<std::string, 10> foo = {"foo1", "foo2", "foo3", "foo3", "foo3", "foo6", "foo7", "foo7", "foo7", "foo10"};

	while (true) {

		ret = SQLFetch(hstmt);
		if (ret == SQL_NO_DATA) {
			break;
		}
		if (ret == SQL_SUCCESS) {
			REQUIRE(long_value == id[row_num]);
			REQUIRE(STR_EQUAL(char_value, foo[row_num].c_str()));
		}

		row_num++;

		// unbind the text field on row 3
		if (row_num == 3) {
			EXECUTE_AND_CHECK("SQLBindCol (HSTMT)", SQLBindCol, hstmt, 2, SQL_C_CHAR, nullptr, 0, nullptr);
		}
		// rebind the text field on row 5 and 9
		if (row_num == 5 || row_num == 9) {
			EXECUTE_AND_CHECK("SQLBindCol (HSTMT)", SQLBindCol, hstmt, 2, SQL_C_CHAR, &char_value, sizeof(char_value),
			                  &ind_char_value);
		}
		// unbind both fields on row 7 using SQLFreeStmt(SQL_UNBIND)
		if (row_num == 7) {
			EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_UNBIND);
		}
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
