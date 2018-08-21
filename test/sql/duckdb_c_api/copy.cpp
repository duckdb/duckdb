
#include "catch.hpp"

#include <vector>

#include "duckdb_c_test.hpp"
#include <fstream>

using namespace std;

TEST_CASE("Test Copy statement", "[copystatement]") {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_result result;

	// open and close a database in in-memory mode
	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);

	// Generate CSV file With ; as delimiter and complex strings
	ofstream from_csv_file("test.csv");
	for (int i = 0; i < 5000; i++)
		from_csv_file << i << "," << i << ", test" << endl;
	from_csv_file.close();
	// Loading CSV into a table
	REQUIRE(
	    duckdb_query(connection,
	                 "CREATE TABLE test (a INTEGER, b INTEGER,c VARCHAR(10));",
	                 NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "COPY test FROM 'test.csv';", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {5000}));
	duckdb_destroy_result(result);

	REQUIRE(duckdb_query(connection, "SELECT COUNT(a), SUM(a) FROM test;",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {5000}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 1, {12497500}));
	duckdb_destroy_result(result);

	//  Creating CSV from table
	REQUIRE(duckdb_query(connection, "COPY test to 'test2.csv';", &result) ==
	        DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {5000}));

	//  Creating CSV from Query
	REQUIRE(duckdb_query(
	            connection,
	            "COPY (select a,b from test where a < 4000) to 'test3.csv';",
	            &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {4000}));

	// Exporting selected columns from a table to a CSV.
	REQUIRE(duckdb_query(connection, "COPY test(a,c) to 'test4.csv';",
	                     &result) == DuckDBSuccess);
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {5000}));

	// Importing CSV to Selected Columns
	REQUIRE(
	    duckdb_query(connection,
	                 "CREATE TABLE test2 (a INTEGER, b INTEGER,c VARCHAR(10));",
	                 NULL) == DuckDBSuccess);
	REQUIRE(duckdb_query(connection, "COPY test2(a,c) from 'test4.csv';",
	                     &result) == DuckDBSuccess);

	REQUIRE(duckdb_disconnect(connection) == DuckDBSuccess);
	REQUIRE(duckdb_close(database) == DuckDBSuccess);

	remove("test.csv");
	remove("test2.csv");
	remove("test3.csv");
	remove("test4.csv");
}
