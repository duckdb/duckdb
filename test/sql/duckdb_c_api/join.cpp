
#include "catch.hpp"

#include <vector>

#include "duckdb_c_test.hpp"

using namespace std;

TEST_CASE("Test basic joins of tables", "[joins]") {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_result result;

	// open and close a database in in-memory mode
	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);

	// create tables
	REQUIRE(duckdb_query(connection,
	                     "CREATE TABLE test (a INTEGER, b INTEGER);",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (11, 1)", NULL) ==
	        DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (12, 2)", NULL) ==
	        DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (13, 3)", NULL) ==
	        DuckDBSuccess);

	REQUIRE(duckdb_query(connection,
	                     "CREATE TABLE test2 (b INTEGER, c INTEGER);",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test2 VALUES (1, 10)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test2 VALUES (1, 20)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test2 VALUES (2, 30)",
	                     NULL) == DuckDBSuccess);

	// NULL selection
	REQUIRE(duckdb_query(
	            connection,
	            "SELECT a, test.b, c FROM test, test2 WHERE test.b = test2.b;",
	            &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {11, 11, 12}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {1, 1, 2}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 2, {10, 20, 30}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_disconnect(connection) == DuckDBSuccess);
	REQUIRE(duckdb_close(database) == DuckDBSuccess);
}
