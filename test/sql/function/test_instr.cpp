#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Instr test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR, off INTEGER, length INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello', 1, 2), "
	                          "('world', 2, 3), ('b', 1, 1), (NULL, 2, 2)"));


	result = con.Query("SELECT instr(s,'h') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 0, 0, Value()}));/*

	result = con.Query("SELECT instr(s,'e') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 0, 0, Value()}));
	
	result = con.Query("SELECT instr(s,'ello') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 0, 0, Value()}));

	result = con.Query("SELECT instr(s,'he') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 0, 0, Value()}));

    result = con.Query("SELECT instr(s,'lo') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 0, 0, Value()}));

	result = con.Query("SELECT instr(s,'he-man') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 0, Value()}));

    /*result = con.Query("SELECT instr(s,'o'),s FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 0, 2, Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "world", "b", Value()}));*/

}