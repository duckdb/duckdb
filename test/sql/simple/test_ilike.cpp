#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar ILIKE statement", "[ilike]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// scalar ILIKE
	result = con.Query("SELECT 'aaa' ILIKE 'bbb'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT 'aaa' ILIKE 'bBb'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT 'aaa' ILIKE 'aaa'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aAa' ILIKE 'aaa'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' ILIKE 'aAa'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' ILIKE '%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' ILIKE '%A'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' ILIKE '%b'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT 'aaa' ILIKE 'a%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' ILIKE 'b%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT 'aaa' ILIKE 'a_A'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' ILIKE 'a_'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT 'aaa' ILIKE '__%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' ILIKE '____%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT 'aBaBaC' ILIKE '%abac'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aBaBaC' NOT ILIKE '%abac'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));
}

TEST_CASE("Test ILIKE statement", "[ilike]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s STRING, pat STRING);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('abab', 'ab%'), "
	                          "('aaa', 'a_a'), ('aaa', '%b%')"));

	result = con.Query("SELECT s FROM strings WHERE s ILIKE 'Ab%'");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab"}));

	result = con.Query("SELECT s FROM strings WHERE 'aba' ILIKE pat");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab", "aaa", "aaa"}));

	result = con.Query("SELECT s FROM strings WHERE s ILIKE pat");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab", "aaa"}));
}
