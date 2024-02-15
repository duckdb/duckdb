#include "include/connect_helpers.h"

#include <iostream>
#include <odbcinst.h>
using namespace odbc_test;

void CheckConfig(SQLHANDLE &dbc, const std::string &setting, const std::string &expected_content) {
	HSTMT hstmt = SQL_NULL_HSTMT;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Check if the setting is successfully changed
	EXECUTE_AND_CHECK("SQLExecDirect (select current_setting('" + setting + "'))", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("select current_setting('" + setting + "')"), SQL_NTS);

	// Fetch the first row
	EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, duckdb::StringUtil::Lower(expected_content));

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

void CheckDatabase(SQLHANDLE &dbc) {
	HSTMT hstmt = SQL_NULL_HSTMT;
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Select * from customers
	EXECUTE_AND_CHECK("SQLExecDirect (SELECT * FROM customer)", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT * FROM customer"), SQL_NTS);

	// Fetch the first row
	idx_t i = 1;
	while (SQLFetch(hstmt) == SQL_SUCCESS) {
		// Fetch the next row
		DATA_CHECK(hstmt, 1, std::to_string(i++));
	}
	REQUIRE(i == 15001);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
}
