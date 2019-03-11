#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar LIKE statement", "[like]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// scalar like
	result = con.Query("SELECT 'aaa' LIKE 'bbb'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT 'aaa' LIKE 'aaa'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' LIKE '%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' LIKE '%a'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' LIKE '%b'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT 'aaa' LIKE 'a%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' LIKE 'b%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT 'aaa' LIKE 'a_a'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' LIKE 'a_'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT 'aaa' LIKE '__%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'aaa' LIKE '____%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT 'ababac' LIKE '%abac'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT 'ababac' NOT LIKE '%abac'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));
}

TEST_CASE("Test LIKE statement", "[like]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s STRING, pat STRING);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('abab', 'ab%'), "
	                          "('aaa', 'a_a'), ('aaa', '%b%')"));

	result = con.Query("SELECT s FROM strings WHERE s LIKE 'ab%'");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab"}));

	result = con.Query("SELECT s FROM strings WHERE 'aba' LIKE pat");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab", "aaa", "aaa"}));

	result = con.Query("SELECT s FROM strings WHERE s LIKE pat");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab", "aaa"}));
}
