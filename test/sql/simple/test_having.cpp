#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test HAVING clause", "[having]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)");

	// HAVING with condition on group
	result = con.Query("SELECT b, SUM(a) AS sum FROM test GROUP BY b HAVING "
	                   "b=21 ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));

	// HAVING with condition on sum
	result = con.Query("SELECT b, SUM(a) FROM test GROUP BY b HAVING SUM(a) < 20 ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));

	// HAVING with condition on ALIAS
	// CONTROVERSIAL: this DOES work in SQLite, but not in PostgreSQL
	REQUIRE_FAIL(con.Query("SELECT b, SUM(a) AS sum FROM test GROUP BY b HAVING sum < 20 ORDER BY b;"));

	// HAVING without alias
	result = con.Query("SELECT b, SUM(a) AS sum FROM test GROUP BY b HAVING "
	                   "SUM(a) < 20 ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	REQUIRE(result->column_count() == 2);

	// HAVING on column not in aggregate
	result = con.Query("SELECT b, SUM(a) AS sum FROM test GROUP BY b HAVING "
	                   "COUNT(*) = 1 ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	REQUIRE(result->column_count() == 2);

	// expression in having
	result = con.Query("SELECT b, SUM(a) FROM test GROUP BY b HAVING SUM(a)+10>28;");
	REQUIRE(CHECK_COLUMN(result, 0, {22}));
	REQUIRE(CHECK_COLUMN(result, 1, {24}));
	REQUIRE(result->column_count() == 2);

	// uncorrelated subquery in having
	result = con.Query("SELECT b, SUM(a) FROM test GROUP BY b HAVING SUM(a)>(SELECT SUM(t.a)*0.5 FROM test t);");
	REQUIRE(CHECK_COLUMN(result, 0, {22}));
	REQUIRE(CHECK_COLUMN(result, 1, {24}));
	REQUIRE(result->column_count() == 2);

	// correlated subquery in having
	result = con.Query(
	    "SELECT b, SUM(a) FROM test GROUP BY b HAVING SUM(a)=(SELECT SUM(a) FROM test t WHERE test.b=t.b) ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 24}));
	REQUIRE(result->column_count() == 2);

	// use outer aggregation in inner subquery
	result = con.Query(
	    "SELECT b, SUM(a) FROM test GROUP BY b HAVING SUM(a)*2=(SELECT SUM(a)+SUM(t.a) FROM test t WHERE test.b=t.b) ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 24}));
	REQUIRE(result->column_count() == 2);

	// use outer aggregation that hasn't been used yet in subquery
	result = con.Query(
	    "SELECT b, SUM(a) FROM test GROUP BY b HAVING SUM(a)*2+2=(SELECT SUM(a)+SUM(t.a)+COUNT(t.a) FROM test t WHERE test.b=t.b) ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {22}));
	REQUIRE(CHECK_COLUMN(result, 1, {24}));
	REQUIRE(result->column_count() == 2);

	// ORDER BY subquery
	result = con.Query(
	    "SELECT b, SUM(a) FROM test GROUP BY b ORDER BY (SELECT SUM(a) FROM test t WHERE test.b=t.b) DESC;");
	REQUIRE(CHECK_COLUMN(result, 0, {22, 21}));
	REQUIRE(CHECK_COLUMN(result, 1, {24, 12}));
	REQUIRE(result->column_count() == 2);
}

TEST_CASE("Test HAVING clause without GROUP BY", "[having]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// scalar HAVING queries
	// CONTROVERSIAL: this works in PostgreSQL, but not in SQLite
	REQUIRE_FAIL(con.Query("SELECT 42 HAVING 42 > 20"));
	REQUIRE_FAIL(con.Query("SELECT 42 HAVING 42 > 80"));

	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)");

	// HAVING with column references
	// CONTROVERSIAL: this works in PostgreSQL, but not in SQLite
	REQUIRE_FAIL(con.Query("SELECT a FROM test WHERE a=13 HAVING a > 11"));

	// HAVING with single-node aggregation does work, even without GROUP BY
	// CONTROVERSIAL: this works in PostgreSQL, but not in SQLite (SQLite requires GROUP BY)
	result = con.Query("SELECT SUM(a) FROM test HAVING SUM(a)>10;");
	REQUIRE(CHECK_COLUMN(result, 0, {36}));

	result = con.Query("SELECT SUM(a) FROM test HAVING SUM(a)<10;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	result = con.Query("SELECT SUM(a) FROM test HAVING COUNT(*)>1;");
	REQUIRE(CHECK_COLUMN(result, 0, {36}));

	result = con.Query("SELECT SUM(a) FROM test HAVING COUNT(*)>10;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

}