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
	CONNECT_TO_DATABASE(ret, env, dbc);

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLAllocHandle (HSTMT)");

	ret = SQLBindCol(hstmt, 1, SQL_C_LONG, &long_value, sizeof(SQLINTEGER), &ind_long_value);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLBindCol (HSTMT)");

	ret = SQLBindCol(hstmt, 2, SQL_C_CHAR, &char_value, sizeof(char_value), &ind_char_value);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLBindCol (HSTMT)");

	ret = SQLExecDirect(hstmt, (SQLCHAR *)"SELECT id, 'foo' || id FROM generate_series(1, 10) id(id)", SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLExecDirect (HSTMT)");

	SQLINTEGER id = 0;
	SQLINTEGER foo = 0;
	SQLINTEGER rowno = 0;
	bool incr_id = true;
	bool incr_foo = true;

	while (1) {

		if (incr_id) {
			id = rowno + 1;
		}
		if (incr_foo) {
			foo = rowno + 1;
		}

		ret = SQLFetch(hstmt);
		if (ret == SQL_NO_DATA) {
			break;
		}
		if (ret == SQL_SUCCESS) {
			REQUIRE(long_value == id);
			auto expected = "foo" + std::to_string(foo);
			REQUIRE(strcmp(char_value, ("foo" + std::to_string(foo)).c_str()) == 0);
			ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFetch (HSTMT)");
		}

		rowno++;

		// unbind the text field on row 3
		if (rowno == 3) {
			ret = SQLBindCol(hstmt, 2, SQL_C_CHAR, NULL, 0, NULL);
			ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLBindCol (HSTMT)");
			incr_foo = false;
		}
		// rebind the text field on row 5 and 9
		if (rowno == 5 || rowno == 9) {
			ret = SQLBindCol(hstmt, 2, SQL_C_CHAR, &char_value, sizeof(char_value), &ind_char_value);
			ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLBindCol (HSTMT)");
			incr_foo = true;
		}
		// unbind both fields on row 7 using SQLFreeStmt(SQL_UNBIND)
		if (rowno == 7) {
			ret = SQLFreeStmt(hstmt, SQL_UNBIND);
			ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFreeStmt (HSTMT)");
			incr_id = false;
			incr_foo = false;
		}
	}

	ret = SQLFreeStmt(hstmt, SQL_CLOSE);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFreeStmt (HSTMT)");

	ret = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFreeHandle (HSTMT)");

	DISCONNECT_FROM_DATABASE(ret, env, dbc);
}
