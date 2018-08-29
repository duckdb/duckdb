
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar NULL handling", "[nullhandling]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// scalar NULL
	result = con.Query("SELECT NULL");
	CHECK_COLUMN(result, 0, {Value()});

	// scalar NULL addition
	result = con.Query("SELECT 3 + NULL");
	CHECK_COLUMN(result, 0, {Value()});

	// division by zero
	result = con.Query("SELECT 4 / 0");
	CHECK_COLUMN(result, 0, {Value()});
}

TEST_CASE("Test simple NULL handling", "[nullhandling]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// multiple insertions
	result = con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	result = con.Query("INSERT INTO test VALUES (11, 22)");
	result = con.Query("INSERT INTO test VALUES (NULL, 21)");
	result = con.Query("INSERT INTO test VALUES (13, 22)");

	// NULL selection
	result = con.Query("SELECT a FROM test");
	CHECK_COLUMN(result, 0, {11, Value(), 13});

	// cast NULL
	result = con.Query("SELECT cast(a AS BIGINT) FROM test;");
	CHECK_COLUMN(result, 0, {11, Value(), 13});

	// NULL addition results in NULL
	result = con.Query("SELECT a + b FROM test");
	CHECK_COLUMN(result, 0, {33, Value(), 35});
}

TEST_CASE("Test NULL handling in aggregations", "[nullhandling]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// multiple insertions
	result = con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	result = con.Query("INSERT INTO test VALUES (11, 22)");
	result = con.Query("INSERT INTO test VALUES (NULL, 21)");
	result = con.Query("INSERT INTO test VALUES (13, 22)");

	// aggregations should ignore NULLs
	result = con.Query("SELECT SUM(a), MIN(a), MAX(a) FROM test");
	CHECK_COLUMN(result, 0, {24});
	CHECK_COLUMN(result, 1, {11});
	CHECK_COLUMN(result, 2, {13});

	// count should ignore NULL
	result = con.Query("SELECT COUNT(*), COUNT(a), COUNT(b) FROM test");
	CHECK_COLUMN(result, 0, {3}); // * returns full table count
	CHECK_COLUMN(result, 1, {2}); // counting "a" ignores null values
	CHECK_COLUMN(result, 2, {3});

	// with GROUP BY as well
	result = con.Query("SELECT b, COUNT(a), SUM(a), MIN(a), MAX(a) FROM test "
	                   "GROUP BY b ORDER BY b");
	CHECK_COLUMN(result, 0, {21, 22});
	CHECK_COLUMN(result, 1, {0, 2});
	CHECK_COLUMN(result, 2, {Value(), 24});
	CHECK_COLUMN(result, 3, {Value(), 11});
	CHECK_COLUMN(result, 4, {Value(), 13});

	// GROUP BY null value
	result = con.Query("INSERT INTO test VALUES (12, NULL)");
	result = con.Query("INSERT INTO test VALUES (16, NULL)");

	result = con.Query("SELECT b, COUNT(a), SUM(a), MIN(a), MAX(a) FROM test "
	                   "GROUP BY b ORDER BY b");
	CHECK_COLUMN(result, 0, {Value(), 21, 22});
	CHECK_COLUMN(result, 1, {2, 0, 2});
	CHECK_COLUMN(result, 2, {28, Value(), 24});
	CHECK_COLUMN(result, 3, {12, Value(), 11});
	CHECK_COLUMN(result, 4, {16, Value(), 13});

	// NULL values should be ignored entirely in the aggregation
	result = con.Query("INSERT INTO test VALUES (NULL, NULL)");
	result = con.Query("INSERT INTO test VALUES (NULL, 22)");

	result = con.Query("SELECT b, COUNT(a), SUM(a), MIN(a), MAX(a) FROM test "
	                   "GROUP BY b ORDER BY b");
	CHECK_COLUMN(result, 0, {Value(), 21, 22});
	CHECK_COLUMN(result, 1, {2, 0, 2});
	CHECK_COLUMN(result, 2, {28, Value(), 24});
	CHECK_COLUMN(result, 3, {12, Value(), 11});
	CHECK_COLUMN(result, 4, {16, Value(), 13});
}