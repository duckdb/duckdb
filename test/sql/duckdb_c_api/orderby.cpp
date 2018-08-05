
#include "catch.hpp"

#include <vector>

#include "duckdb_c_test.hpp"

using namespace std;

TEST_CASE("Test order by statements", "[orderby]") {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_result result;

	// open and close a database in in-memory mode
	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);

	// multiple insertions
	REQUIRE(duckdb_query(connection, "CREATE TABLE test (a INTEGER, b INTEGER);", NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (11, 22)", NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (12, 21)", NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (13, 22)", NULL) == DuckDBSuccess);

	REQUIRE(duckdb_query(connection, "SELECT a, b FROM test ORDER BY a;", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {22, 21, 22}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT a, b FROM test ORDER BY a DESC;", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {13, 12, 11}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {22, 21, 22}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT a, b FROM test ORDER BY b, a;", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {12, 11, 13}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {21, 22, 22}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT a, b FROM test ORDER BY b DESC, a;", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {11, 13, 12}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {22, 22, 21}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT a, b FROM test ORDER BY b, a DESC;", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {12, 13, 11}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {21, 22, 22}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT a, b FROM test ORDER BY b, a DESC LIMIT 1;", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {12}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {21}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT a, b FROM test ORDER BY b, a DESC LIMIT 1 OFFSET 1;", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {13}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {22}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT a, b FROM test WHERE a < 13 ORDER BY b;", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {12, 11}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {21, 22}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT a, b FROM test WHERE a < 13 ORDER BY b DESC;", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {11, 12}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {22, 21}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT b, a FROM test WHERE a < 13 ORDER BY b DESC;", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {22, 21}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {11, 12}));
	duckdb_destroy_result(result);

	// REQUIRE(duckdb_query(connection, "SELECT b % 2 AS f, SUM(a) FROM test GROUP BY f ORDER BY f;", &result) == DuckDBSuccess);
	// REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {0, 1}));
	// REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {24, 12}));
	// duckdb_destroy_result(result);


	// REQUIRE(duckdb_query(connection, "SELECT b % 2 AS f, SUM(a) FROM test GROUP BY f ORDER BY b % 2;", &result) == DuckDBSuccess);
	// REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {0, 1}));
	// REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {24, 12}));
	// duckdb_destroy_result(result);

	REQUIRE(duckdb_disconnect(connection) == DuckDBSuccess);
	REQUIRE(duckdb_close(database) == DuckDBSuccess);
}
