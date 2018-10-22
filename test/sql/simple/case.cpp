
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test case statement", "[case]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)");

	result =
	    con.Query("SELECT CASE WHEN test.a=11 THEN b ELSE NULL END FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {22, Value(), Value()}));
}

TEST_CASE("Test NULL IF statement", "[case]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
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
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value("world"), Value("world"), Value("world")}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));

	// result = con.Query("SELECT NULLIF(CAST(a AS VARCHAR), 11) FROM test;");
	// REQUIRE(CHECK_COLUMN(result, 0, {Value(11), Value(13), Value(12)}));
}
