
#include "catch.hpp"

#include <vector>

#include "duckdb_c_test.hpp"

using namespace std;

TEST_CASE("Test aggregation/group by by statements", "[aggregations]") {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_result result;

	// open and close a database in in-memory mode
	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);

	// multiple insertions
	REQUIRE(duckdb_query(connection,
	                     "CREATE TABLE test (a INTEGER, b INTEGER);",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (11, 22)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (12, 21)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (13, 22)",
	                     NULL) == DuckDBSuccess);

	// aggregations without group by statements
	REQUIRE(duckdb_query(connection, "SELECT SUM(41), COUNT(*);", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {41}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {1}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT SUM(a), COUNT(*), AVG(a) FROM test;",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {36}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_DECIMAL_COLUMN(result, 2, {12.0}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection,
	                     "SELECT SUM(a), COUNT(*) FROM test WHERE a = 11;",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {11}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {1}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection,
	                     "SELECT SUM(a), SUM(b), SUM(a) + SUM (b) FROM test;",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {36}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {65}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 2, {101}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection,
	                     "SELECT SUM(a+2), SUM(a) + 2 * COUNT(*) FROM test;",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {42}));
	duckdb_destroy_result(result);

	// aggregations with group by
	REQUIRE(duckdb_query(
	            connection,
	            "SELECT b, SUM(a), SUM(a+2) FROM test GROUP BY b ORDER BY b;",
	            &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {12, 24}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 2, {14, 28}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection,
	                     "SELECT b, SUM(a), COUNT(*), SUM(a+2) FROM test GROUP "
	                     "BY b ORDER BY b;",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {12, 24}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 2, {1, 2}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 3, {14, 28}));
	duckdb_destroy_result(result);

	// group by alias
	REQUIRE(duckdb_query(connection,
	                     "SELECT b % 2 AS f, SUM(a) FROM test GROUP BY f;",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {24, 12}));
	duckdb_destroy_result(result);

	// REQUIRE(duckdb_query(connection, "SELECT b, AVG(a) FROM test GROUP BY
	// b;", &result) == DuckDBSuccess);

	REQUIRE(duckdb_disconnect(connection) == DuckDBSuccess);
	REQUIRE(duckdb_close(database) == DuckDBSuccess);
}
