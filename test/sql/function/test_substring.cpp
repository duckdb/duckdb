#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Substring test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR, off INTEGER, length INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello', 1, 2), "
	                          "('world', 2, 3), ('b', 1, 1), (NULL, 2, 2)"));

    // test zero length
	result = con.Query("SELECT SUBSTRING('🦆ab', 1, 0), SUBSTRING('abc', 1, 0)");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));
	REQUIRE(CHECK_COLUMN(result, 1, {""}));

	// constant offset/length
	// normal substring
	result = con.Query("SELECT substring(s from 1 for 2) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"he", "wo", "b", Value()}));

	// substring out of range
	result = con.Query("SELECT substring(s from 2 for 2) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"el", "or", "", Value()}));

	// variable length offset/length
	result = con.Query("SELECT substring(s from off for length) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"he", "orl", "b", Value()}));

	result = con.Query("SELECT substring(s from off for 2) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"he", "or", "b", Value()}));

	result = con.Query("SELECT substring(s from 1 for length) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"he", "wor", "b", Value()}));

	result = con.Query("SELECT substring('hello' from off for length) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"he", "ell", "h", "el"}));

	// test substrings with constant nulls in different places
	result = con.Query("SELECT substring(NULL from off for length) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
	result = con.Query("SELECT substring('hello' from NULL for length) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
	result = con.Query("SELECT substring('hello' from off for NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
	result = con.Query("SELECT substring(NULL from NULL for length) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
	result = con.Query("SELECT substring('hello' from NULL for NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
	result = con.Query("SELECT substring(NULL from off for NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
	result = con.Query("SELECT substring(NULL from NULL for NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
}

TEST_CASE("Substring test with UTF8", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('two"
	                          "\xc3\xb1"
	                          "three"
	                          "\xE2\x82\xA1"
	                          "four"
	                          "\xF0\x9F\xA6\x86"
	                          "end')"));

	result = con.Query("SELECT substring(s from 1 for 7) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"two"
	                      "\xc3\xb1"
	                      "thr"}));

	result = con.Query("SELECT substring(s from 10 for 7) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"\xE2\x82\xA1"
	                      "four"
	                      "\xF0\x9F\xA6\x86"
	                      "e"}));

	result = con.Query("SELECT substring(s from 15 for 7) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"\xF0\x9F\xA6\x86"
	                      "end"}));
}
