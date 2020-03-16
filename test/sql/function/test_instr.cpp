#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

/* Test Case disclaimer
*
*  Assertions built using the Domain Testing technique
*  at: https://bbst.courses/wp-content/uploads/2018/01/Kaner-Intro-to-Domain-Testing-2018.pdf
*
*/
TEST_CASE("Instr test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR, off INTEGER, length INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello', 1, 2), "
	                          "('world', 2, 3), ('b', 1, 1), (NULL, 2, 2)"));

	// Test first letter
	result = con.Query("SELECT instr(s,'h') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 0, 0, Value()}));

	// Test second letter
	result = con.Query("SELECT instr(s,'e') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 0, 0, Value()}));

	// Test last letter
	result = con.Query("SELECT instr(s,'d') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 5, 0, Value()}));

	// Test multiple letters
	result = con.Query("SELECT instr(s,'he') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 0, 0, Value()}));

	// Test multiple letters in the middle
	result = con.Query("SELECT instr(s,'ello') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 0, 0, Value()}));

	// Test multiple letters at the end
    result = con.Query("SELECT instr(s,'lo') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 0, 0, Value()}));

	// Test no match
	result = con.Query("SELECT instr(s,'he-man') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 0, Value()}));

    // Test matching needle in multiple rows
    result = con.Query("SELECT instr(s,'o'),s FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 2, 0, Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "world", "b", Value()}));

	// Test NULL constant in different places
	result = con.Query("SELECT instr(NULL,'o') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
	result = con.Query("SELECT instr(s,NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
	result = con.Query("SELECT instr(NULL,NULL) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
}