
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Table subquery", "[subquery]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	con.Query("CREATE TABLE test (i INTEGER, j INTEGER)");
	con.Query("INSERT INTO test VALUES (3, 4), (4, 5), (5, 6);");

	result = con.Query(
	    "SELECT * FROM (SELECT i, j AS d FROM test ORDER BY i) AS b;");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5, 6}));

	// check column names for simple projections and aliases
	result =
	    con.Query("SELECT b.d FROM (SELECT i * 2 + j AS d FROM test) AS b;");
	REQUIRE(CHECK_COLUMN(result, 0, {10, 13, 16}));

	// join with subqueries
	result =
	    con.Query("SELECT a.i,a.j,b.r,b.j FROM (SELECT i, j FROM test) AS a "
	              "INNER JOIN (SELECT i+1 AS r,j FROM test) AS b ON a.i=b.r;");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 6}));
	REQUIRE(CHECK_COLUMN(result, 2, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 3, {4, 5}));

	// check that * is in the correct order
	result = con.Query(
	    "SELECT * FROM (SELECT i, j FROM test) AS a, (SELECT "
	    "i+1 AS r,j FROM test) AS b, test WHERE a.i=b.r AND test.j=a.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 6}));
	REQUIRE(CHECK_COLUMN(result, 2, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 3, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 4, {3, 4}));
	REQUIRE(CHECK_COLUMN(result, 5, {4, 5}));
}
