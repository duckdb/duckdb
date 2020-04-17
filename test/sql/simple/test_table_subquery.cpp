#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Table subquery", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3, 4), (4, 5), (5, 6);"));

	result = con.Query("SELECT * FROM (SELECT i, j AS d FROM test ORDER BY i) AS b;");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5, 6}));

	// check column names for simple projections and aliases
	result = con.Query("SELECT b.d FROM (SELECT i * 2 + j AS d FROM test) AS b;");
	REQUIRE(CHECK_COLUMN(result, 0, {10, 13, 16}));

	// join with subqueries
	result = con.Query("SELECT a.i,a.j,b.r,b.j FROM (SELECT i, j FROM test) AS a "
	                   "INNER JOIN (SELECT i+1 AS r,j FROM test) AS b ON a.i=b.r ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 6}));
	REQUIRE(CHECK_COLUMN(result, 2, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 3, {4, 5}));

	// check that * is in the correct order
	result = con.Query("SELECT * FROM (SELECT i, j FROM test) AS a, (SELECT "
	                   "i+1 AS r,j FROM test) AS b, test WHERE a.i=b.r AND test.j=a.i ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 6}));
	REQUIRE(CHECK_COLUMN(result, 2, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 3, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 4, {3, 4}));
	REQUIRE(CHECK_COLUMN(result, 5, {4, 5}));

	// subquery group cols are visible
	result = con.Query("select sum(x) from (select i as x from test group by i) sq;");
	REQUIRE(CHECK_COLUMN(result, 0, {12}));

	// subquery group aliases are visible
	result = con.Query("select sum(x) from (select i+1 as x from test group by x) sq;");
	REQUIRE(CHECK_COLUMN(result, 0, {15}));
}

TEST_CASE("Nested table subquery", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	con.Query("CREATE TABLE test (i INTEGER, j INTEGER)");
	con.Query("INSERT INTO test VALUES (3, 4), (4, 5), (5, 6);");

	result = con.Query("SELECT * FROM (SELECT i, j FROM (SELECT j AS i, i AS j FROM (SELECT j "
	                   "AS i, i AS j FROM test) AS a) AS a) AS a, (SELECT i+1 AS r,j FROM "
	                   "test) AS b, test WHERE a.i=b.r AND test.j=a.i ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 6}));
	REQUIRE(CHECK_COLUMN(result, 2, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 3, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 4, {3, 4}));
	REQUIRE(CHECK_COLUMN(result, 5, {4, 5}));

	const int NESTING_LEVELS = 100;

	// 100 nesting levels
	string query = "SELECT i FROM ";
	for (size_t i = 0; i < NESTING_LEVELS; i++) {
		query += "(SELECT i + 1 AS i FROM ";
	}
	query += "test";
	for (size_t i = 0; i < NESTING_LEVELS; i++) {
		query += ") AS a";
	}
	query += ";";
	result = con.Query(query.c_str());
	REQUIRE(CHECK_COLUMN(result, 0, {3 + NESTING_LEVELS, 4 + NESTING_LEVELS, 5 + NESTING_LEVELS}));
}
