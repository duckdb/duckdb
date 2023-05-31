#include "../common.h"
#include <iostream>

using namespace odbc_test;

TEST_CASE("bindcol", "[odbc") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(ret, env, dbc);
	DISCONNECT_FROM_DATABASE(ret, dbc, env);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect (HDBC)");

	// Connect to the database using SQLDriverConnect
	string dsn = "DuckDB";
	auto envvar = getenv("COMMON_CONNECTION_STRING_FOR_REGRESSION_TEST");
    if (envvar != NULL && envar[0] != '\0') {
        dsn = "DSN=" + dsn + ";" + envvar;
	}
}
