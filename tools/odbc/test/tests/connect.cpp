#include "../common.h"
#include <iostream>

using namespace odbc_test;

TEST_CASE("connect", "[odbc") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(ret, env, dbc);
	DISCONNECT_FROM_DATABASE(ret, dbc, env);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect");

	// Connect to the database using SQLDriverConnect
	DRIVER_CONNECT_TO_DATABASE(ret, env, dbc, "");
	DISCONNECT_FROM_DATABASE(ret, dbc, env);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect");
}
