#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar SIMILAR TO statement", "[similar]") {
	unique_ptr<QueryResult> result;
	DuckDB database(nullptr);
	Connection connection(database);

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'aaa'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'bbb'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO '%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'a%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO '%a'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO '%b'");`
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'b%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'a_a'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'a_'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'a_%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO '__%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO '___%'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = connection.Query("SELECT 'aaa' NOT SIMILAR TO '___'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = connection.Query("SELECT 'aaa' ~ 'aaa'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));
    
	result = connection.Query("SELECT 'aaa' ~ '^a'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' ~ '(a|b)'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));
    
	result = connection.Query("SELECT 'aaa' ~ '^(a|b)'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

}

TEST_CASE("Test SIMILAR TO statement", "[similar]") {
	unique_ptr<QueryResult> result;
	DuckDB database(nullptr);
	Connection connection(database);

	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE strings (s STRING, p STRING);"));
	REQUIRE_NO_FAIL(
	    connection.Query("INSERT INTO strings VALUES('aaa', 'a_a'), ('abab', 'ab%'), ('aaa', 'a_a'), ('aaa', '%b%');"));
	result = connection.Query("");

	result = connection.Query("SELECT s FROM strings WHERE s SIMILAR TO 'ab%'");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab"}));

	result = connection.Query("SELECT s FROM strings WHERE 'aba' SIMILAR TO p");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab", "aaa", "aaa"}));

	result = connection.Query("SELECT s FROM strings WHERE s SIMILAR TO p");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab", "aaa"}));
}
