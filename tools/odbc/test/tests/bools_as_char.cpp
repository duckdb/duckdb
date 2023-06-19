#include "../common.h"

using namespace odbc_test;

TEST_CASE("bools_as_char", "[odbc]") {
	//	SQLRETURN ret;
	//	SQLHANDLE env;
	//	SQLHANDLE dbc;
	//	HSTMT hstmt = SQL_NULL_HSTMT;
	//
	//	// Connect to the database
	//	CONNECT_TO_DATABASE(ret, env, dbc);
	//
	//	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);
	//	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLAllocHandle (HSTMT)");
	//
	//	ret = SQLFreeStmt(hstmt, SQL_CLOSE);
	//	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFreeStmt (HSTMT)");
	//
	//	ret = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
	//	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLFreeHandle (HSTMT)");
	//
	//	DISCONNECT_FROM_DATABASE(ret, dbc, env);
	//	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect (HDBC)");
}
