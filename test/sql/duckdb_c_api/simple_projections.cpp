
#include "catch.hpp"

#include <vector>

#include "duckdb_c_test.hpp"

using namespace std;

TEST_CASE("Test scalar queries", "[scalarquery]") {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_result result;

	// open and close a database in in-memory mode
	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);

	REQUIRE(duckdb_query(connection, "SELECT 42", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC(result, 0, 0, 42));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT 42 + 1", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC(result, 0, 0, 43));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT 2 * (42 + 1), 35 - 2", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC(result, 0, 0, 86));
	REQUIRE(CHECK_NUMERIC(result, 0, 1, 33));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT 'hello'", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_STRING(result, 0, 0, "hello"));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT cast('3' AS INTEGER)", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC(result, 0, 0, 3));
	duckdb_destroy_result(result);

	// Incorrect queries
	// unterminated string
	REQUIRE(duckdb_query(connection, "SELECT 'hello", &result) !=
	        DuckDBSuccess);
	// cast string to integer
	// (should this fail or return 0? SQLite returns 0)
	REQUIRE(duckdb_query(connection, "SELECT cast('hello' AS INTEGER)",
	                     &result) != DuckDBSuccess);

	REQUIRE(duckdb_disconnect(connection) == DuckDBSuccess);
	REQUIRE(duckdb_close(database) == DuckDBSuccess);
}
