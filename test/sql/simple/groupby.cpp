
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test aggregation/group by by statements", "[aggregations]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)");

	result = con.Query("SELECT SUM(41), COUNT(*);");
	REQUIRE(CHECK_COLUMN(result, 0, {41}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));

	result = con.Query("SELECT SUM(a), COUNT(*), AVG(a) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {36}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {12.0}));

	result = con.Query("SELECT COUNT(*) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	result = con.Query("SELECT SUM(a), COUNT(*) FROM test WHERE a = 11;");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));

	result = con.Query("SELECT SUM(a), SUM(b), SUM(a) + SUM (b) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {36}));
	REQUIRE(CHECK_COLUMN(result, 1, {65}));
	REQUIRE(CHECK_COLUMN(result, 2, {101}));

	result = con.Query("SELECT SUM(a+2), SUM(a) + 2 * COUNT(*) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {42}));

	// aggregations with group by
	result = con.Query(
	    "SELECT b, SUM(a), SUM(a+2), AVG(a) FROM test GROUP BY b ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 24}));
	REQUIRE(CHECK_COLUMN(result, 2, {14, 28}));
	REQUIRE(CHECK_COLUMN(result, 3, {12, 12}));

	result = con.Query("SELECT b, SUM(a), COUNT(*), SUM(a+2) FROM test GROUP "
	                   "BY b ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 24}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {14, 28}));

	// group by alias
	result = con.Query("SELECT b % 2 AS f, SUM(a) FROM test GROUP BY f;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {24, 12}));

	// group by with filter
	result = con.Query("SELECT b, SUM(a), COUNT(*), SUM(a+2) FROM test WHERE "
	                   "a <= 12 GROUP "
	                   "BY b ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 11}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 1}));
	REQUIRE(CHECK_COLUMN(result, 3, {14, 13}));

	con.Query("INSERT INTO test VALUES (12, 21), (12, 21), (12, 21)");

	// group by with filter and multiple values per groups
	result = con.Query("SELECT b, SUM(a), COUNT(*), SUM(a+2) FROM test WHERE "
	                   "a <= 12 GROUP "
	                   "BY b ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {12 * 4, 11}));
	REQUIRE(CHECK_COLUMN(result, 2, {4, 1}));
	REQUIRE(CHECK_COLUMN(result, 3, {12 * 4 + 2 * 4, 13}));

	// group by with filter and multiple values per groups
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO integers VALUES (3, 4), (3, 4), (2, 4);"));

	// use GROUP BY column in math operator
	result = con.Query("SELECT i, i + 10 FROM integers GROUP BY i ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 13}));

	// using non-group column and non-aggregate should translate to the FIRST()
	// aggregate
	result =
	    con.Query("SELECT i, SUM(j), j FROM integers GROUP BY i ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 8}));
	REQUIRE(CHECK_COLUMN(result, 2, {4, 4}));
}

TEST_CASE("GROUP BY large strings", "[aggregations]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('helloworld', 22), "
	                          "('thisisalongstring', 22), ('helloworld', 21)"));

	result = con.Query("SELECT a, SUM(b) FROM test GROUP BY a ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"helloworld", "thisisalongstring"}));
	REQUIRE(CHECK_COLUMN(result, 1, {43, 22}));
}

TEST_CASE("Aggregate only COUNT STAR", "[aggregations]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO integers VALUES (3, 4), (3, 4), (2, 4);"));

	result =
	    con.Query("SELECT i, COUNT(*) FROM integers GROUP BY i ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
}

TEST_CASE("DISTINCT aggregations", "[aggregations]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE distinctagg(i INTEGER, j INTEGER);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO distinctagg VALUES (1,1),(1,1),(2,2), (1,2)"));

//	result = con.Query("SELECT COUNT(i), COUNT(DISTINCT i), SUM(i), "
//	                   "SUM(DISTINCT i) FROM distinctagg");
//
//		REQUIRE(CHECK_COLUMN(result, 0, {4}));
//	REQUIRE(CHECK_COLUMN(result, 1, {2}));
//	REQUIRE(CHECK_COLUMN(result, 2, {5}));
//	REQUIRE(CHECK_COLUMN(result, 3, {3}));

	result =
	    con.Query("SELECT COUNT(i), COUNT(DISTINCT i), SUM(i), SUM(DISTINCT i) "
	              "FROM distinctagg GROUP BY j ORDER BY j");

	REQUIRE(CHECK_COLUMN(result, 0, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {1, 3}));
}
