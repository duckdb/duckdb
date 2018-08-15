
#include "catch.hpp"

#include <vector>

#include "duckdb_c_test.hpp"

using namespace std;

TEST_CASE("Test simple projection statements", "[simpleprojection]") {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_result result;

	// open and close a database in in-memory mode
	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);

	// create table
	REQUIRE(duckdb_query(connection, "CREATE TABLE a (i integer, j integer);",
	                     NULL) == DuckDBSuccess);
	// insertion: 1 affected row
	REQUIRE(duckdb_query(connection, "INSERT INTO a VALUES (42, 84);",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC(result, 0, 0, 1));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT * FROM a;", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC(result, 0, 0, 42));
	REQUIRE(CHECK_NUMERIC(result, 0, 1, 84));
	duckdb_destroy_result(result);

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

	// multiple projections
	REQUIRE(duckdb_query(connection, "SELECT a, b FROM test;", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC(result, 0, 0, 11));
	REQUIRE(CHECK_NUMERIC(result, 0, 1, 22));
	REQUIRE(CHECK_NUMERIC(result, 1, 0, 12));
	REQUIRE(CHECK_NUMERIC(result, 1, 1, 21));
	REQUIRE(CHECK_NUMERIC(result, 2, 0, 13));
	REQUIRE(CHECK_NUMERIC(result, 2, 1, 22));
	duckdb_destroy_result(result);

	// basic expressions and filters
	REQUIRE(duckdb_query(connection, "SELECT a + 2, b FROM test WHERE a = 11;",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC(result, 0, 0, 13));
	REQUIRE(CHECK_NUMERIC(result, 0, 1, 22));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT a + 2, b FROM test WHERE a = 12;",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC(result, 0, 0, 14));
	REQUIRE(CHECK_NUMERIC(result, 0, 1, 21));
	duckdb_destroy_result(result);

	// // select unknown column
	// REQUIRE(duckdb_query(connection, "SELECT c FROM test;", &result) !=
	//         DuckDBSuccess);

	REQUIRE(duckdb_disconnect(connection) == DuckDBSuccess);
	REQUIRE(duckdb_close(database) == DuckDBSuccess);
}
