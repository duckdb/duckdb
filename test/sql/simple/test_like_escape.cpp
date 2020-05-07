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

	result = con.Query("SELECT '\' LIKE '\\' ESCAPE '\';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT '\\' LIKE '\\' ESCAPE '\';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT '%' LIKE '*%' ESCAPE '*';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT '_ ' LIKE '*_ ' ESCAPE '*';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT ' a ' LIKE '*_ ' ESCAPE '*';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = con.Query("SELECT '\%_' LIKE '\%_' ESCAPE '';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = con.Query("SELECT '\%_' LIKE '\%_';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	REQUIRE_FAIL(con.Query("SELECT '\%_' LIKE '\%_' ESCAPE '\\';"));
}

TEST_CASE("Test LIKE statement with ESCAPE", "[like]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s STRING, pat STRING);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('abab', 'ab%'), "
	                          "('aaa', 'a*_a'), ('aaa', '*%b'), ('bbb', 'a%');"));

	result = con.Query("SELECT s FROM strings WHERE pat LIKE 'a*%' ESCAPE '*'");
	REQUIRE(CHECK_COLUMN(result, 0, {"bbb"}));

	result = con.Query("SELECT s FROM strings WHERE 'aba' LIKE pat ESCAPE '*'");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab"}));

	result = con.Query("SELECT s FROM strings WHERE s LIKE pat ESCAPE '*'");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab"}));
}
