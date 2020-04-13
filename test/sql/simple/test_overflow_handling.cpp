#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test handling of overflows in basic types", "[overflowhandling]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));

	//! Casting NULL should still work though
	REQUIRE_NO_FAIL(con.Query("SELECT ALL CAST ( - SUM ( DISTINCT - CAST ( "
	                          "NULL AS INTEGER ) ) AS INTEGER ) FROM test"));

	// insert too large value for domain should cause error
	// FIXME new pg parser zeroes first param
	REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (-1099511627776, 3)"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (12, 21), (14, 22)"));

	//! Casting NULL should still work though
	REQUIRE_NO_FAIL(con.Query("SELECT ALL CAST ( - SUM ( DISTINCT - CAST ( "
	                          "NULL AS INTEGER ) ) AS INTEGER ) FROM test"));

	// proper upcasting of integer columns in AVG
	result = con.Query("SELECT b, AVG(a) FROM test GROUP BY b ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));

	// FIXME: statistics propagation for overflows needs to be reintroduced
	return;
	// cast to bigger type if it will overflow
	result = con.Query("SELECT cast(200 AS TINYINT)");
	REQUIRE(CHECK_COLUMN(result, 0, {200}));

	// try to use the NULL value of a type
	result = con.Query("SELECT cast(-127 AS TINYINT)");
	REQUIRE(CHECK_COLUMN(result, 0, {-127}));

	// promote on addition overflow
	result = con.Query("SELECT cast(100 AS TINYINT) + cast(100 AS TINYINT)");
	REQUIRE(CHECK_COLUMN(result, 0, {200}));

	// also with tables
	result = con.Query("CREATE TABLE test2 (a INTEGER, b TINYINT);");
	result = con.Query("INSERT INTO test2 VALUES (200, 60), (12, 60), (14, 60)");

	// cast to bigger type if it will overflow
	result = con.Query("SELECT cast(a AS TINYINT) FROM test2");
	REQUIRE(CHECK_COLUMN(result, 0, {200, 12, 14}));

	// cast to bigger type if SUM overflows
	result = con.Query("SELECT SUM(b) FROM test2");
	REQUIRE(CHECK_COLUMN(result, 0, {180}));

	// promote overflows in more complicated expression chains
	// FIXME: need to fix statistics propagation
	result = con.Query("SELECT a + b FROM (SELECT cast(100 AS TINYINT) AS a, cast(100 AS TINYINT) AS b) tbl1");
	REQUIRE(CHECK_COLUMN(result, 0, {200}));
}

TEST_CASE("Test handling of overflows in float/double", "[overflowhandling]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// out of range constants are not accepted
	REQUIRE_FAIL(con.Query("SELECT 1e1000"));
	// overflow on cast from double to real results in an error
	REQUIRE_FAIL(con.Query("SELECT 1e308::REAL"));
	// test string casts
	REQUIRE_FAIL(con.Query("SELECT '1e1000'::DOUBLE"));
	REQUIRE_FAIL(con.Query("SELECT '1e100'::REAL"));

	// overflow in SUM/AVG results in an error
	REQUIRE_FAIL(con.Query("SELECT SUM(i) FROM (VALUES (1e308), (1e308)) tbl(i)"));
	REQUIRE_FAIL(con.Query("SELECT AVG(i) FROM (VALUES (1e308), (1e308)) tbl(i)"));

	// overflow in arithmetic as well
	REQUIRE_FAIL(con.Query("SELECT 1e308+1e308"));
	REQUIRE_FAIL(con.Query("SELECT 1e308*2"));
	REQUIRE_FAIL(con.Query("SELECT -1e308-1e308"));
	REQUIRE_FAIL(con.Query("SELECT 1e308/0.1"));

	REQUIRE_FAIL(con.Query("SELECT 2e38::REAL+2e38::REAL"));
	REQUIRE_FAIL(con.Query("SELECT 2e38::REAL*2"));
	REQUIRE_FAIL(con.Query("SELECT -2e38::REAL-2e38::REAL"));
	REQUIRE_FAIL(con.Query("SELECT 2e38::REAL/0.1::REAL"));
}
