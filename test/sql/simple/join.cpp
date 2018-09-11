
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test basic joins of tables", "[joins]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	// create tables
	result = con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	result = con.Query("INSERT INTO test VALUES (11, 1)");
	result = con.Query("INSERT INTO test VALUES (12, 2)");
	result = con.Query("INSERT INTO test VALUES (13, 3)");

	result = con.Query("CREATE TABLE test2 (b INTEGER, c INTEGER);");
	result = con.Query("INSERT INTO test2 VALUES (1, 10)");
	result = con.Query("INSERT INTO test2 VALUES (1, 20)");
	result = con.Query("INSERT INTO test2 VALUES (2, 30)");

	// simple cross product + join condition
	result = con.Query(
	    "SELECT a, test.b, c FROM test, test2 WHERE test.b = test2.b;");
	CHECK_COLUMN(result, 0, {11, 11, 12});
	CHECK_COLUMN(result, 1, {1, 1, 2});
	CHECK_COLUMN(result, 2, {10, 20, 30});

	// use join columns in subquery
	result = con.Query("SELECT a, (SELECT test.a), c FROM test, test2 WHERE "
	                   "test.b = test2.b;");
	CHECK_COLUMN(result, 0, {11, 11, 12});
	CHECK_COLUMN(result, 1, {11, 11, 12});
	CHECK_COLUMN(result, 2, {10, 20, 30});

	// explicit join
	result = con.Query(
	    "SELECT a, test.b, c FROM test INNER JOIN test2 ON test.b = test2.b;");
	CHECK_COLUMN(result, 0, {11, 11, 12});
	CHECK_COLUMN(result, 1, {1, 1, 2});
	CHECK_COLUMN(result, 2, {10, 20, 30});

	// explicit join with condition the wrong way around
	result = con.Query(
	    "SELECT a, test.b, c FROM test INNER JOIN test2 ON test2.b = test.b;");
	CHECK_COLUMN(result, 0, {11, 11, 12});
	CHECK_COLUMN(result, 1, {1, 1, 2});
	CHECK_COLUMN(result, 2, {10, 20, 30});

}
