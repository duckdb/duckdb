#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test concat function", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello'), ('world'), (NULL)"));

	// normal concat
	result = con.Query("SELECT s || ' ' || s FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello hello", "world world"}));

	// unicode concat
	result = con.Query("SELECT s || ' ' || '🦆' FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello 🦆", "world 🦆"}));

	// varargs concat
	result = con.Query("SELECT s || ' ' || '🦆' FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello 🦆", "world 🦆"}));

	// concat with constant NULL
	result = con.Query("SELECT s || ' ' || '🦆' || NULL FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));

	// concat requires at least one argument
	REQUIRE_FAIL(con.Query("SELECT CONCAT()"));

	// concat with one argument works
	result = con.Query("SELECT CONCAT('hello')");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));

	// automatic casting also works for vargs
	result = con.Query("SELECT CONCAT('hello', 33, 22)");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello3322"}));

	// CONCAT ignores null values
	result = con.Query("SELECT CONCAT('hello', 33, NULL, 22, NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello3322"}));
	// this also applies to non-constant null values
	result = con.Query("SELECT CONCAT('hello', ' ', s) FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello ", "hello hello", "hello world"}));
}

TEST_CASE("Test length function", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello'), ('world'), (NULL)"));

	// normal length
	result = con.Query("SELECT length(s) FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 5, 5}));

	// length after concat
	result = con.Query("SELECT length(s || ' ' || '🦆') FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 7, 7}));
}
