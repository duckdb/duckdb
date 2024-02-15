#include "connect_helpers.h"

using namespace odbc_test;

// If running this test on macOS you need to run the following script before executing the test
// tools/odbc/scripts/create_ini_file.sh

// Connect to the database using the ini file
TEST_CASE("Test SQLConnect with Ini File", "[odbc]") {
#if defined ODBC_LINK_ODBCINST || defined WIN32
	// Connect to the database using the ini file
	SQLHANDLE env;
	SQLHANDLE dbc;
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "DSN=DuckDB");

	// Check that the database is set
	CheckDatabase(dbc);

	// Check that database is read only
	CheckConfig(dbc, "access_mode", "read_only");

	// Check that allow_unsigned_extensions is set
	CheckConfig(dbc, "allow_unsigned_extensions", "true");

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
#endif
}
