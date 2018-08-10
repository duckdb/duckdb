
#include "catch.hpp"

#include <vector>

#include "duckdb_c_test.hpp"

using namespace std;

TEST_CASE("Test NULL handling of basic types", "[nullhandling]") {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_result result;

	// open and close a database in in-memory mode
	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);

	// scalar NULL
	REQUIRE(duckdb_query(connection, "SELECT NULL", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {NULL_NUMERIC}));
	duckdb_destroy_result(result);

	// scalar NULL addition
	REQUIRE(duckdb_query(connection, "SELECT 3 + NULL", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {NULL_NUMERIC}));
	duckdb_destroy_result(result);

	// division by zero
	REQUIRE(duckdb_query(connection, "SELECT 4 / 0", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {NULL_NUMERIC}));
	duckdb_destroy_result(result);

	// multiple insertions
	REQUIRE(duckdb_query(connection,
	                     "CREATE TABLE test (a INTEGER, b INTEGER);",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (11, 22)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (NULL, 21)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (13, 22)",
	                     NULL) == DuckDBSuccess);

	// NULL selection
	REQUIRE(duckdb_query(connection, "SELECT a FROM test", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {11, NULL_NUMERIC, 13}));
	duckdb_destroy_result(result);

	// cast NULL
	REQUIRE(duckdb_query(connection, "SELECT cast(a AS BIGINT) FROM test;",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {11, NULL_NUMERIC, 13}));
	duckdb_destroy_result(result);

	// NULL addition results in NULL
	REQUIRE(duckdb_query(connection, "SELECT a + b FROM test", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {33, NULL_NUMERIC, 35}));
	duckdb_destroy_result(result);

	// aggregations should ignore NULLs
	REQUIRE(duckdb_query(connection, "SELECT SUM(a), MIN(a), MAX(a) FROM test",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {24}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {11}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 2, {13}));
	duckdb_destroy_result(result);

	// count should ignore NULL
	REQUIRE(duckdb_query(connection,
	                     "SELECT COUNT(*), COUNT(a), COUNT(b) FROM test",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {3})); // * returns full table count
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1,
	                             {2})); // counting "a" ignores null values
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 2, {3}));
	duckdb_destroy_result(result);

	// with GROUP BY as well
	REQUIRE(duckdb_query(connection,
	                     "SELECT b, COUNT(a), SUM(a), MIN(a), MAX(a) FROM test "
	                     "GROUP BY b ORDER BY b",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {0, 2}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 2, {NULL_NUMERIC, 24}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 3, {NULL_NUMERIC, 11}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 4, {NULL_NUMERIC, 13}));
	duckdb_destroy_result(result);

	// GROUP BY null value
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (12, NULL)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (16, NULL)",
	                     NULL) == DuckDBSuccess);

	REQUIRE(duckdb_query(connection,
	                     "SELECT b, COUNT(a), SUM(a), MIN(a), MAX(a) FROM test "
	                     "GROUP BY b ORDER BY b",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {NULL_NUMERIC, 21, 22}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {2, 0, 2}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 2, {28, NULL_NUMERIC, 24}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 3, {12, NULL_NUMERIC, 11}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 4, {16, NULL_NUMERIC, 13}));
	duckdb_destroy_result(result);

	// NULL values should be ignored entirely in the aggregation
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (NULL, NULL)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (NULL, 22)",
	                     NULL) == DuckDBSuccess);

	REQUIRE(duckdb_query(connection,
	                     "SELECT b, COUNT(a), SUM(a), MIN(a), MAX(a) FROM test "
	                     "GROUP BY b ORDER BY b",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {NULL_NUMERIC, 21, 22}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {2, 0, 2}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 2, {28, NULL_NUMERIC, 24}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 3, {12, NULL_NUMERIC, 11}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 4, {16, NULL_NUMERIC, 13}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_disconnect(connection) == DuckDBSuccess);
	REQUIRE(duckdb_close(database) == DuckDBSuccess);
}
