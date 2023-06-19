#include "../common.h"

using namespace odbc_test;

TEST_CASE("Test SQLConnect and SQLDriverConnect", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);
	DISCONNECT_FROM_DATABASE(env, dbc);

	// Connect to the database using SQLDriverConnect
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "");
	DISCONNECT_FROM_DATABASE(env, dbc);
}
