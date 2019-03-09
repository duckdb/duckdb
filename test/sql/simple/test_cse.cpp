#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test queries involving Common SubExpressions", "[cse]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table test(a integer);"));
	REQUIRE_NO_FAIL(con.Query("insert into test values (42);"));

	// single CSE
	result = con.Query("SELECT (a*2)+(a*2) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {168}));
	// multiple CSE
	result = con.Query("SELECT (a*2)+(a*2)+(a*2)+(a*2)+(a*2) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {420}));

	// CSE in WHERE clause
	result = con.Query("SELECT * FROM test WHERE ((a*2)+(a*2))>100");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	// multiple CSE in WHERE clause
	result = con.Query("SELECT * FROM test WHERE ((a*2)+(a*2)+(a*2)+(a*2)+(a*2))>400");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("Test queries involving Common SubExpressions and Strings and NULL values", "[cse]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table test(a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("insert into test values ('hello'), ('world'), (NULL);"));

	// single CSE in projection
	result = con.Query("SELECT substring(a, 1, 3)=substring(a, 1, 3) FROM test ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true, true}));

	// now with GROUP BY clause
	result = con.Query("SELECT substring(a, 1, 3)=substring(a, 1, 3) AS b FROM test GROUP BY b ORDER BY b");
	// result->Print();
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true}));
}
