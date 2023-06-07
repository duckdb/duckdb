#include "../common.h"

#include <iostream>

using namespace odbc_test;

TEST_CASE("bind_col", "[odbc") {
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
	ExecuteCmdAndCheckODBC("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Bind the first column (long_value) to a long
	ExecuteCmdAndCheckODBC("SQLBindCol (HSTMT)", SQLBindCol, hstmt, 1, SQL_C_LONG, &long_value, sizeof(SQLINTEGER),
	                       &ind_long_value);

	// Bind the second column (char_value) to a string
	ExecuteCmdAndCheckODBC("SQLBindCol (HSTMT)", SQLBindCol, hstmt, 2, SQL_C_CHAR, &char_value, sizeof(char_value),
	                       &ind_char_value);

	// Execute the query
	ExecuteCmdAndCheckODBC("SQLExecDirect (HSTMT)", SQLExecDirect, hstmt,
	                       ConvertToSQLCHAR("SELECT id, 'foo' || id FROM generate_series(1, 10) id(id)"), SQL_NTS);

	SQLINTEGER id = 0;
	SQLINTEGER foo = 0;
	SQLINTEGER row_num = 0;
	bool incr_id = true;
	bool incr_foo = true;

	while (true) {

		if (incr_id) {
			id = row_num + 1;
		}
		if (incr_foo) {
			foo = row_num + 1;
		}

		ret = SQLFetch(hstmt);
		if (ret == SQL_NO_DATA) {
			break;
		}
		if (ret == SQL_SUCCESS) {
			REQUIRE(long_value == id);
			REQUIRE(strcmp(char_value, ("foo" + std::to_string(foo)).c_str()) == 0);
		}

		row_num++;

		// unbind the text field on row 3
		if (row_num == 3) {
			ExecuteCmdAndCheckODBC("SQLBindCol (HSTMT)", SQLBindCol, hstmt, 2, SQL_C_CHAR, nullptr, 0, nullptr);
			incr_foo = false;
		}
		// rebind the text field on row 5 and 9
		if (row_num == 5 || row_num == 9) {
			ExecuteCmdAndCheckODBC("SQLBindCol (HSTMT)", SQLBindCol, hstmt, 2, SQL_C_CHAR, &char_value,
			                       sizeof(char_value), &ind_char_value);
			incr_foo = true;
		}
		// unbind both fields on row 7 using SQLFreeStmt(SQL_UNBIND)
		if (row_num == 7) {
			ExecuteCmdAndCheckODBC("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_UNBIND);
			incr_id = false;
			incr_foo = false;
		}
	}

	// Free the statement handle
	ExecuteCmdAndCheckODBC("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	ExecuteCmdAndCheckODBC("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
