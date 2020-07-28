#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test basic comparison statements", "[comparison]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// = and == are the same
	result = con.Query("SELECT 1 == 1, 1 = 1, 1 == 0, 1 = 0, 1 == NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	REQUIRE(CHECK_COLUMN(result, 1, {true}));
	REQUIRE(CHECK_COLUMN(result, 2, {false}));
	REQUIRE(CHECK_COLUMN(result, 3, {false}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));

	// != and <> are the same
	result = con.Query("SELECT 1 <> 1, 1 != 1, 1 <> 0, 1 != 0, 1 <> NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	REQUIRE(CHECK_COLUMN(result, 1, {false}));
	REQUIRE(CHECK_COLUMN(result, 2, {true}));
	REQUIRE(CHECK_COLUMN(result, 3, {true}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
}

TEST_CASE("Test auto casting of comparison statements", "[comparison]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// string <> number comparisons should result in the string being cast to a number
	REQUIRE_FAIL(con.Query("select ('abc' between 20 and True);"));
	REQUIRE_FAIL(con.Query("select 'abc' > 10"));
	REQUIRE_FAIL(con.Query("select 20.0 = 'abc'"));

	// 1000 > 20
	result = con.Query("select '1000' > 20");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	// ... but '1000' < '20'!
	result = con.Query("select '1000' > '20'");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	result = con.Query("select ('abc' between '20' and 'true');");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
}
