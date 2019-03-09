#include "catch.hpp"
#include "duckdb.h"

using namespace std;

TEST_CASE("Basic test of C API", "[capi]") {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_result result;

	// open and close a database in in-memory mode
	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);

	// select scalar value
	REQUIRE(duckdb_query(connection, "SELECT CAST(42 AS BIGINT)", &result) == DuckDBSuccess);
	REQUIRE(result.column_count == 1);
	REQUIRE(result.row_count == 1);
	REQUIRE(duckdb_value_int64(&result, 0, 0) == 42);
	REQUIRE(!result.columns[0].nullmask[0]);
	duckdb_destroy_result(&result);

	// select scalar NULL
	REQUIRE(duckdb_query(connection, "SELECT NULL", &result) == DuckDBSuccess);
	REQUIRE(result.column_count == 1);
	REQUIRE(result.row_count == 1);
	REQUIRE(result.columns[0].nullmask[0]);
	duckdb_destroy_result(&result);

	// select scalar string
	REQUIRE(duckdb_query(connection, "SELECT 'hello'", &result) == DuckDBSuccess);
	REQUIRE(result.column_count == 1);
	REQUIRE(result.row_count == 1);
	auto value = duckdb_value_varchar(&result, 0, 0);
	string strval = string(value);
	free((void*)value);
	REQUIRE(strval == "hello");
	REQUIRE(!result.columns[0].nullmask[0]);
	duckdb_destroy_result(&result);

	// multiple insertions
	REQUIRE(duckdb_query(connection, "CREATE TABLE test (a INTEGER, b INTEGER);", NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (11, 22)", NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (NULL, 21)", NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "INSERT INTO test VALUES (13, 22)", NULL) == DuckDBSuccess);

	// NULL selection
	REQUIRE(duckdb_query(connection, "SELECT a, b FROM test ORDER BY a", &result) == DuckDBSuccess);
	// NULL, 11, 13
	REQUIRE(result.columns[0].nullmask[0]);
	REQUIRE(duckdb_value_int32(&result, 0, 1) == 11);
	REQUIRE(duckdb_value_int32(&result, 0, 2) == 13);
	// 21, 22, 22
	REQUIRE(duckdb_value_int32(&result, 1, 0) == 21);
	REQUIRE(duckdb_value_int32(&result, 1, 1) == 22);
	REQUIRE(duckdb_value_int32(&result, 1, 2) == 22);
	duckdb_destroy_result(&result);

	// close database
	duckdb_disconnect(&connection);
	duckdb_close(&database);
}
