#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar SIMILAR TO statement", "[similar]") {
	unique_ptr<QueryResult> result;
	DuckDB database(nullptr);
	Connection connection(database);

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'bbb'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'aaa'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO '.*'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'a.*'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO '.*a'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO '.*b'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'b.*'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'a[a-z]a'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'a[a-z]{2}'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO 'a[a-z].*'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO '[a-z][a-z].*'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' SIMILAR TO '[a-z]{3}'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' NOT SIMILAR TO '[b-z]{3}'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' ~ 'aaa'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' !~ 'bbb'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	// similar to must match entire expression
	result = connection.Query("SELECT 'aaa' ~ '^a'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));
	result = connection.Query("SELECT 'aaa' ~ '^a+'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'aaa' ~ '(a|b)*'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(true)}));

	result = connection.Query("SELECT 'abc' ~ '^(b|c)'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(false)}));
}

TEST_CASE("Test SIMILAR TO statement with expressions", "[similar]") {
	unique_ptr<QueryResult> result;
	DuckDB database(nullptr);
	Connection connection(database);

	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE strings (s STRING, p STRING);"));
	REQUIRE_NO_FAIL(connection.Query(
	    "INSERT INTO strings VALUES('aaa', 'a[a-z]a'), ('abab', 'ab.*'), ('aaa', 'a[a-z]a'), ('aaa', '.*b.*');"));
	result = connection.Query("");

	result = connection.Query("SELECT s FROM strings WHERE s SIMILAR TO 'ab.*'");
	REQUIRE(CHECK_COLUMN(result, 0, {"abab"}));

	result = connection.Query("SELECT s FROM strings WHERE 'aba' SIMILAR TO p");
	REQUIRE(CHECK_COLUMN(result, 0, {"aaa", "abab", "aaa", "aaa"}));

	result = connection.Query("SELECT s FROM strings WHERE s SIMILAR TO p");
	REQUIRE(CHECK_COLUMN(result, 0, {"aaa", "abab", "aaa"}));

	result = connection.Query("SELECT s FROM strings WHERE s NOT SIMILAR TO p");
	REQUIRE(CHECK_COLUMN(result, 0, {"aaa"}));
}

TEST_CASE("Test SIMILAR TO statement exceptions", "[similar]") {
	unique_ptr<QueryResult> result;
	DuckDB database(nullptr);
	Connection connection(database);

	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE strings (s STRING, p STRING);"));
	REQUIRE_NO_FAIL(connection.Query(
	    "INSERT INTO strings VALUES('aaa', 'a[a-z]a'), ('abab', 'ab.*'), ('aaa', 'a[a-z]a'), ('aaa', '.*b.*');"));
	result = connection.Query("");

	REQUIRE_FAIL(connection.Query("SELECT s FROM strings WHERE s SIMILAR TO 'ab.*\%' {escape '\'}"));
}
