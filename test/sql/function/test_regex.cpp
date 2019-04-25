#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("search test", "[regex]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// constant strings
	result = con.Query("SELECT regexp_matches('asdf', '.*sd.*')");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	result = con.Query("SELECT regexp_matches('asdf', '.*yu.*')");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	result = con.Query("SELECT regexp_matches('asdf', '')");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	// full matches only
	result = con.Query("SELECT regexp_matches('asdf', 'sd')");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	// empty strings
	result = con.Query("SELECT regexp_matches('', '.*yu.*')");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	result = con.Query("SELECT regexp_matches('', '.*')");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	// NULLs
	result = con.Query("SELECT regexp_matches('asdf', CAST(NULL AS STRING))");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT regexp_matches(CAST(NULL AS STRING), '.*sd.*')");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT regexp_matches(CAST(NULL AS STRING), CAST(NULL AS STRING))");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// postgres says throw error on invalid regex
	REQUIRE_FAIL(con.Query("SELECT regexp_matches('', '\\X')"));


//	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s STRING, p STRING)"));
//	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('Hello', 'World')"));
//
//
//	result = con.Query("SELECT regexp_matches(s, '') FROM strings");
//	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello hello", "world world"}));
//


}
