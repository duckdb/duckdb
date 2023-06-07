#include "../common.h"

#include <iostream>

using namespace odbc_test;

TEST_CASE("select", "[odbc") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	ExecuteCmdAndCheckODBC("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Execute a simple query
	ExecuteCmdAndCheckODBC("SQLExecDirect (SELECT 1 UNION ALL SELECT 2)", SQLExecDirect, hstmt,
	                       ConvertToSQLCHAR("SELECT 1 UNION ALL SELECT 2"), SQL_NTS);

	// Fetch the first row
	ExecuteCmdAndCheckODBC("SQLFetch (SELECT 1 UNION ALL SELECT 2)", SQLFetch, hstmt);
	// Check the data
	DATA_CHECK(hstmt, 1, "1");

	// Fetch the second row
	ExecuteCmdAndCheckODBC("SQLFetch (SELECT 1 UNION ALL SELECT 2)", SQLFetch, hstmt);
	// Check the data
	DATA_CHECK(hstmt, 1, "2");

	ExecuteCmdAndCheckODBC("SQLFreeStmt (SQL_CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);

	// Creat a query with 1600 columns
	std::string query = "SELECT ";
	for (int i = 1; i < 1600; i++) {
		query += std::to_string(i);
		if (i < 1599) {
			query += ", ";
		}
	}

	ExecuteCmdAndCheckODBC("SQLExecDirect (SELECT 1600 columns)", SQLExecDirect, hstmt, ConvertToSQLCHAR(query.c_str()),
	                       SQL_NTS);

	// Fetch the first row
	ExecuteCmdAndCheckODBC("SQLFetch (SELECT 1600 columns)", SQLFetch, hstmt);

	// Check the data
	for (int i = 1; i < 1600; i++) {
		DATA_CHECK(hstmt, i, std::to_string(i).c_str());
	}

	// Free the statement handle
	ExecuteCmdAndCheckODBC("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	ExecuteCmdAndCheckODBC("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
