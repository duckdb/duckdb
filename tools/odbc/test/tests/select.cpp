#include "../common.h"

#include <iostream>

using namespace odbc_test;

TEST_CASE("select", "[odbc") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(ret, env, dbc);

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLAllocHandle (HSTMT)");

	// Execute a simple query
	ret = SQLExecDirect(hstmt, (SQLCHAR *)"SELECT 1 UNION ALL SELECT 2", SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLExecDirect (SELECT 1 UNION ALL SELECT 2)");

	// Fetch the first row
	ret = SQLFetch(hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFetch (SELECT 1 UNION ALL SELECT 2)");
	// Check the data
	DATA_CHECK(hstmt, 1, "1");

	// Fetch the second row
	ret = SQLFetch(hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFetch (SELECT 1 UNION ALL SELECT 2)");
	// Check the data
	DATA_CHECK(hstmt, 1, "2");

	ret = SQLFreeStmt(hstmt, SQL_CLOSE);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFreeStmt (SQL_CLOSE)");

	// Creat a query with 1600 columns
	std::string query = "SELECT ";
	for (int i = 1; i < 1600; i++) {
		query += std::to_string(i);
		if (i < 1599) {
			query += ", ";
		}
	}

	ret = SQLExecDirect(hstmt, (SQLCHAR *)query.c_str(), SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLExecDirect (SELECT 1600 columns)");

	// Fetch the first row
	ret = SQLFetch(hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFetch (SELECT 1600 columns)");

	// Check the data
	for (int i = 1; i < 1600; i++) {
		DATA_CHECK(hstmt, i, std::to_string(i).c_str());
	}

	// Free the statement handle
	ret = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFreeHandle (HSTMT)");

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(ret, env, dbc);
}
