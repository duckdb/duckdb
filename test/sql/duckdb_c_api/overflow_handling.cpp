
#include "catch.hpp"

#include <vector>

#include "duckdb_c_test.hpp"

using namespace std;

TEST_CASE("Test handling of overflows in basic types", "[overflowhandling]") {
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
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (14, 22)",
	                     NULL) == DuckDBSuccess);

	// proper upcasting of integer columns in AVG
	REQUIRE(duckdb_query(connection, "SELECT b, AVG(a) FROM test GROUP BY b ORDER BY b;", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_DECIMAL_COLUMN(result, 0, {12, 12.5}));
	duckdb_destroy_result(result);
	

	// cast to bigger type if it will overflow
	REQUIRE(duckdb_query(connection, "SELECT cast(200 AS TINYINT)", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {200}));
	duckdb_destroy_result(result);

	// try to use the NULL value of a type
	REQUIRE(duckdb_query(connection, "SELECT cast(-127 AS TINYINT)", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {-127}));
	duckdb_destroy_result(result);

	// promote on addition overflow
	REQUIRE(duckdb_query(connection, "SELECT cast(100 AS TINYINT) + cast(100 AS TINYINT)", &result) == DuckDBSuccess);
	duckdb_print_result(result);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {200}));
	duckdb_destroy_result(result);

	// also with tables
	REQUIRE(duckdb_query(connection,
	                     "CREATE TABLE test2 (a INTEGER, b TINYINT);",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test2 VALUES (200, 60)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test2 VALUES (12, 60)",
	                     NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test2 VALUES (14, 60)",
	                     NULL) == DuckDBSuccess);


	// cast to bigger type if it will overflow
	REQUIRE(duckdb_query(connection, "SELECT cast(a AS TINYINT) FROM test2", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {200, 12, 14}));
	duckdb_destroy_result(result);

	// cast to bigger type if SUM overflows
	REQUIRE(duckdb_query(connection, "SELECT SUM(b) FROM test2", &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {180}));
	duckdb_destroy_result(result);
	REQUIRE(duckdb_disconnect(connection) == DuckDBSuccess);
	REQUIRE(duckdb_close(database) == DuckDBSuccess);
}
