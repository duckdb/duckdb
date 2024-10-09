#include "catch.hpp"
#define DUCKDB_API_NO_DEPRECATED

#include "duckdb.h"

using namespace std;

// we only use functions that are cool to use in the 1.0 API
TEST_CASE("Test without deprecated or future moved functions", "[capi]") {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_prepared_statement statement;
	duckdb_result result;

	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);
	REQUIRE(duckdb_prepare(connection, "SELECT ?::INTEGER AS a", &statement) == DuckDBSuccess);
	REQUIRE(duckdb_bind_int32(statement, 1, 42) == DuckDBSuccess);
	REQUIRE(duckdb_execute_prepared(statement, &result) == DuckDBSuccess);

	REQUIRE(duckdb_column_count(&result) == 1);
	REQUIRE(string(duckdb_column_name(&result, 0)) == "a");
	REQUIRE(duckdb_column_type(&result, 0) == DUCKDB_TYPE_INTEGER);

	auto chunk = duckdb_fetch_chunk(result);
	REQUIRE(chunk);
	auto vector = duckdb_data_chunk_get_vector(chunk, 0);
	REQUIRE(vector);
	auto validity = duckdb_vector_get_validity(vector);
	REQUIRE(duckdb_validity_row_is_valid(validity, 0));
	auto data = (int *)duckdb_vector_get_data(vector);
	REQUIRE(data);
	REQUIRE(data[0] == 42);

	duckdb_destroy_data_chunk(&chunk);
	duckdb_destroy_result(&result);
	duckdb_destroy_prepare(&statement);
	duckdb_disconnect(&connection);
	duckdb_close(&database);
}
