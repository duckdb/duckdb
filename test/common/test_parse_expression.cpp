#include "catch.hpp"
#include "duckdb/parser/parser.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Parse Expression List valid expressions", "[parse_expression]") {
	auto result = Parser::ParseExpressionList("x");
	REQUIRE(result.size() == 1);

	result = Parser::ParseExpressionList("x, y, z");
	REQUIRE(result.size() == 3);

	result = Parser::ParseExpressionList("FIRST(x) AS x");
	REQUIRE(result.size() == 1);

	result = Parser::ParseExpressionList("x + 1, y * 2");
	REQUIRE(result.size() == 2);
}

TEST_CASE("Parse Expression List rejects invalid clauses", "[parse_expression]") {
#ifdef DUCKDB_CRASH_ON_ASSERT
	return;
#endif
	REQUIRE_THROWS_AS(Parser::ParseExpressionList("FIRST(x) AS x WHERE x = 'bad'"), ParserException);
	REQUIRE_THROWS_AS(Parser::ParseExpressionList("first(x) AS x HAVING x"), ParserException);
	REQUIRE_THROWS_AS(Parser::ParseExpressionList("first(x) AS x QUALIFY x"), ParserException);
	REQUIRE_THROWS_AS(Parser::ParseExpressionList("first(x) AS x USING SAMPLE 1 ROWS"), ParserException);
	REQUIRE_THROWS_AS(Parser::ParseExpressionList("first(x) AS x GROUP BY x"), ParserException);
	REQUIRE_THROWS_AS(Parser::ParseExpressionList("first(x) AS x ORDER BY x"), ParserException);
	REQUIRE_THROWS_AS(Parser::ParseExpressionList("x LIMIT 1"), ParserException);
}
