#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar LIKE statement with custom ESCAPE", "[like]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// scalar like with escape
	result = con.Query("SELECT '%++' LIKE '*%++' ESCAPE '*';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));
	// Not Like
	result = con.Query("SELECT '%++' NOT LIKE '*%++' ESCAPE '*';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));
	// Default tests
	result = con.Query("SELECT '\\' LIKE '\\\\' ESCAPE '\\';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT '\\\\' LIKE '\\\\' ESCAPE '\\';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT '%' LIKE '*%' ESCAPE '*';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT '_ ' LIKE '*_ ' ESCAPE '*';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT ' a ' LIKE '*_ ' ESCAPE '*';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT '\%_' LIKE '\%_' ESCAPE '';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT '*%' NOT LIKE '*%' ESCAPE '*';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	// It should fail when more than one escape character is specified
	REQUIRE_FAIL(con.Query("SELECT '\%_' LIKE '\%_' ESCAPE '\\\\';"));
	REQUIRE_FAIL(con.Query("SELECT '\%_' LIKE '\%_' ESCAPE '**';"));
}

TEST_CASE("Test LIKE statement with ESCAPE in the middle of the pattern", "[like]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s STRING, pat STRING);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('abab', 'ab%'), "
	                          "('aaa', 'a*_a'), ('aaa', '*%b'), ('bbb', 'a%');"));

	result = con.Query("SELECT s FROM strings;");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab", "aaa", "aaa", "bbb"}));

	result = con.Query("SELECT pat FROM strings;");
	REQUIRE(CHECK_COLUMN(result, 0, {"ab%", "a*_a", "*%b", "a%"}));

	result = con.Query("SELECT s FROM strings WHERE pat LIKE 'a*%' ESCAPE '*';");
	REQUIRE(CHECK_COLUMN(result, 0, {"bbb"}));

	result = con.Query("SELECT s FROM strings WHERE 'aba' LIKE pat ESCAPE '*';");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab", "bbb"}));

	result = con.Query("SELECT s FROM strings WHERE s LIKE pat ESCAPE '*';");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab"}));
}
