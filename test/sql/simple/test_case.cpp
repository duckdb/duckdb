#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test case statement", "[case]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)");

	result = con.Query("SELECT CASE WHEN test.a=11 THEN b ELSE NULL END FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {22, Value(), Value()}));
	// constant case statements
	// all constant
	result = con.Query("SELECT CASE WHEN 1=1 THEN 1 ELSE NULL END FROM test ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1}));
	// check + res_if_false constant
	result = con.Query("SELECT CASE WHEN 1=1 THEN b ELSE NULL END FROM test ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22, 22}));
	// check + res_if_true constant
	result = con.Query("SELECT CASE WHEN 3>2 THEN NULL ELSE b+1 END FROM test ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	// check constant
	result = con.Query("SELECT CASE WHEN 1=0 THEN b ELSE b+1 END FROM test ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {22, 23, 23}));
	// res_if_true and res_if_false constant
	result = con.Query("SELECT CASE WHEN b=22 THEN NULL ELSE 1 END FROM test ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {1, Value(), Value()}));
	// res_if_false constant
	result = con.Query("SELECT CASE WHEN b=22 THEN b+1 ELSE 1 END FROM test ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 23, 23}));
	// res_if_true constant
	result = con.Query("SELECT CASE WHEN b=22 THEN NULL ELSE b+1 END FROM test ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {22, Value(), Value()}));

	// fail case on types that can't be cast to boolean
	REQUIRE_FAIL(con.Query("SELECT CASE WHEN 'hello' THEN b ELSE a END FROM test"));
	// but only when cast cannot be performed
	result = con.Query("SELECT CASE WHEN 'true' THEN NULL ELSE b+1 END FROM test ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	result = con.Query("SELECT CASE WHEN 'false' THEN NULL ELSE b+1 END FROM test ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {22, 23, 23}));
}

TEST_CASE("Test NULL IF statement", "[case]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	// NULL IF
	result = con.Query("SELECT NULLIF(NULLIF ('hello', 'world'), 'blabla');");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("hello")}));

	// NULL IF with subquery
	con.Query("CREATE TABLE test (a STRING);");
	con.Query("INSERT INTO test VALUES ('hello'), ('world'), ('test')");

	con.Query("CREATE TABLE test2 (a STRING, b STRING);");
	con.Query("INSERT INTO test2 VALUES ('blabla', 'b'), ('blabla2', 'c'), "
	          "('blabla3', 'd')");

	result = con.Query("SELECT NULLIF(NULLIF ((SELECT a FROM test "
	                   "LIMIT 1 offset 1), a), b) FROM test2");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("world"), Value("world"), Value("world")}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));
}

TEST_CASE("NULL IF with strings", "[case]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));

	result = con.Query("SELECT NULLIF(CAST(a AS VARCHAR), 11) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value("13"), Value("12")}));

	result =
	    con.Query("SELECT a, CASE WHEN a>11 THEN CAST(a AS VARCHAR) ELSE CAST(b AS VARCHAR) END FROM test ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value("22"), Value("12"), Value("13")}));
}
