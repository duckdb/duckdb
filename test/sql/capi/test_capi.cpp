#include "capi_helpers.hpp"
#include "catch.hpp"

#include <vector>

using namespace std;

TEST_CASE("Basic test of C API", "[capi]") {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_result result;

	// open and close a database in in-memory mode
	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);

	// select scalar NULL
	REQUIRE(duckdb_query(connection, "SELECT 42", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {42}));
	duckdb_destroy_result(result);

	// select scalar NULL
	REQUIRE(duckdb_query(connection, "SELECT NULL", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {NULL_NUMERIC}));
	duckdb_destroy_result(result);

	// multiple insertions
	REQUIRE(duckdb_query(connection, "CREATE TABLE test (a INTEGER, b INTEGER);", NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (11, 22)", NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (NULL, 21)", NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (13, 22)", NULL) == DuckDBSuccess);

	// NULL selection
	REQUIRE(duckdb_query(connection, "SELECT a FROM test", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {11, NULL_NUMERIC, 13}));
	duckdb_destroy_result(result);

	// close database
	REQUIRE(duckdb_disconnect(connection) == DuckDBSuccess);
	REQUIRE(duckdb_close(database) == DuckDBSuccess);
}
