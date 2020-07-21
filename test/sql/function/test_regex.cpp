#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("regex search test", "[regex]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// constant strings
	result = con.Query("SELECT regexp_matches('asdf', '.*sd.*')");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	result = con.Query("SELECT regexp_matches('asdf', '.*yu.*')");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	result = con.Query("SELECT regexp_matches('asdf', '')");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	// partial matches okay
	result = con.Query("SELECT regexp_matches('asdf', 'sd')");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	// full match requires entire match
	result = con.Query("SELECT regexp_full_match('asdf', 'sd')");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT regexp_full_match('asdf', '.sd.')");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	result = con.Query("SELECT regexp_matches('asdf', '^sdf$')");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	// empty strings
	result = con.Query("SELECT regexp_matches('', '.*yu.*')");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	result = con.Query("SELECT regexp_matches('', '.*')");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	// NULLs
	result = con.Query("SELECT regexp_matches('asdf', CAST(NULL AS STRING))");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT regexp_matches(CAST(NULL AS STRING), '.*sd.*')");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT regexp_matches(CAST(NULL AS STRING), CAST(NULL AS STRING))");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT regexp_matches('foobarbequebaz', '(bar)(beque)')");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	// postgres says throw error on invalid regex
	REQUIRE_FAIL(con.Query("SELECT regexp_matches('', '\\X')"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE regex(s STRING, p STRING)"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO regex VALUES ('asdf', 'sd'), ('asdf', '^sd'), (NULL, '^sd'), ('asdf', NULL)"));
	result = con.Query("SELECT regexp_matches(s, '.*') FROM regex");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, Value(), true}));

	result = con.Query("SELECT regexp_matches(s, p) FROM regex");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, Value(), Value()}));
}

TEST_CASE("regex filter push test", "[regex]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE regex(s STRING)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO regex VALUES ('asdf'), ('xxxx'), ('aaaa')"));

	result = con.Query("SELECT s FROM regex WHERE REGEXP_MATCHES(s, 'as(c|d|e)f')");
	REQUIRE(CHECK_COLUMN(result, 0, {"asdf"}));

	result = con.Query("SELECT s FROM regex WHERE NOT REGEXP_MATCHES(s, 'as(c|d|e)f')");
	REQUIRE(CHECK_COLUMN(result, 0, {"xxxx", "aaaa"}));

	result = con.Query("SELECT s FROM regex WHERE REGEXP_MATCHES(s, 'as(c|d|e)f') AND s = 'asdf'");
	REQUIRE(CHECK_COLUMN(result, 0, {"asdf"}));

	result = con.Query("SELECT s FROM regex WHERE REGEXP_MATCHES(s, 'as(c|d|e)f') AND REGEXP_MATCHES(s, 'as[a-z]f')");
	REQUIRE(CHECK_COLUMN(result, 0, {"asdf"}));
}

TEST_CASE("regex replace test", "[regex]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("SELECT regexp_replace('foobarbaz', 'b..', 'X')");
	REQUIRE(CHECK_COLUMN(result, 0, {"fooXbaz"}));
}

TEST_CASE("regex replace with options", "[regex]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// global replace
	result = con.Query("SELECT regexp_replace('ana ana', 'ana', 'banana', 'g')");
	REQUIRE(CHECK_COLUMN(result, 0, {"banana banana"}));
	result = con.Query("SELECT regexp_replace('ANA ana', 'ana', 'banana', 'gi')");
	REQUIRE(CHECK_COLUMN(result, 0, {"banana banana"}));
	// case sensitivity
	result = con.Query("SELECT regexp_replace('ana', 'ana', 'banana', 'c')");
	REQUIRE(CHECK_COLUMN(result, 0, {"banana"}));
	result = con.Query("SELECT regexp_replace('ANA', 'ana', 'banana', 'i')");
	REQUIRE(CHECK_COLUMN(result, 0, {"banana"}));
	// dot matches newline
	result = con.Query("SELECT regexp_replace('hello\nworld', '.*', 'x', 'sg')");
	REQUIRE(CHECK_COLUMN(result, 0, {"x"}));
	result = con.Query("SELECT regexp_replace('hello\nworld', '.*', 'x', 'ng')");
	REQUIRE(CHECK_COLUMN(result, 0, {"x\nx"}));

	// this also works with tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(v VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('hello'), ('HELLO');"));

	result = con.Query("SELECT regexp_replace(v, 'h.*', 'world', 'i') FROM test ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {"world", "world"}));
	result = con.Query("SELECT regexp_replace(v, 'h.*', 'world', 'c') FROM test ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {"HELLO", "world"}));
	// we cannot use non-constant options (currently)
	REQUIRE_FAIL(con.Query("SELECT regexp_replace(v, 'h.*', 'world', v) FROM test ORDER BY v"));

	// throw on invalid options
	REQUIRE_FAIL(con.Query("SELECT regexp_replace('asdf', '.*SD.*', 'a', 'q')"));
}

TEST_CASE("regex match with options", "[regex]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// case sensitivity
	result = con.Query("SELECT regexp_matches('asdf', '.*SD.*', 'i')");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT regexp_matches('asdf', '.*SD.*', 'c')");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	// dot matches newline
	result = con.Query("SELECT regexp_matches('hello\nworld', '.*', 's')");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT regexp_full_match('hello\nworld', '.*', 'n')");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	// whitespace is ignored
	result = con.Query("SELECT regexp_matches('asdf', '.*SD.*', ' i 	')");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	// NULL in options is ignored
	result = con.Query("SELECT regexp_matches('asdf', '.*SD.*', NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	// this also works with tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(v VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('hello'), ('HELLO');"));

	result = con.Query("SELECT regexp_matches(v, 'h.*', 'i') FROM test ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true}));
	result = con.Query("SELECT regexp_matches(v, 'h.*', 'c') FROM test ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true}));
	// we cannot use non-constant options (currently)
	REQUIRE_FAIL(con.Query("SELECT regexp_matches(v, 'h.*', v) FROM test ORDER BY v"));

	// throw on invalid options
	REQUIRE_FAIL(con.Query("SELECT regexp_matches('asdf', '.*SD.*', 'q')"));
	// can only use "g" with regexp replace
	REQUIRE_FAIL(con.Query("SELECT regexp_matches('asdf', '.*SD.*', 'g')"));
}
