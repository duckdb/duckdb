#include "catch.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/span.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Span type semantics", "[parse_expression]") {
	// invalid sentinel (used for synthesized expressions with no source location)
	REQUIRE(!Span().IsValid());
	REQUIRE(!Span::Invalid().IsValid());

	Span s(5, 3);
	REQUIRE(s.IsValid());
	REQUIRE(s.Start() == 5);
	REQUIRE(s.End() == 8);

	// trivial conversion both ways with optional_idx
	optional_idx as_idx = s;
	REQUIRE(as_idx.GetIndex() == 5);
	Span from_idx = optional_idx(9);
	REQUIRE(from_idx.offset == 9);
	REQUIRE(from_idx.length == 0);
	REQUIRE(!Span(optional_idx()).IsValid());

	// merge produces the smallest enclosing span and ignores invalid spans
	auto merged = Span(2, 2).Merge(Span(6, 1)); // [2,4) U [6,7) -> [2,7)
	REQUIRE(merged.offset == 2);
	REQUIRE(merged.length == 5);
	REQUIRE(Span(2, 2).Merge(Span::Invalid()) == Span(2, 2));
	REQUIRE(Span::Invalid().Merge(Span(2, 2)) == Span(2, 2));
}

TEST_CASE("Parsed expressions carry source spans", "[parse_expression]") {
	// Note: ParseExpressionList wraps the input in a SELECT, so offsets are relative to that wrapper.
	// The span length is what matters here and is independent of the wrapper.

	// a numeric constant spans its own token
	auto result = Parser::ParseExpressionList("42");
	REQUIRE(result.size() == 1);
	auto span = result[0]->GetQueryLocation();
	REQUIRE(span.IsValid());
	REQUIRE(span.length == 2);

	// a column reference encloses the full identifier
	result = Parser::ParseExpressionList("abc");
	span = result[0]->GetQueryLocation();
	REQUIRE(span.IsValid());
	REQUIRE(span.length == 3);

	// a qualified column reference encloses the whole qualified name (tbl.abc)
	result = Parser::ParseExpressionList("tbl.abc");
	span = result[0]->GetQueryLocation();
	REQUIRE(span.IsValid());
	REQUIRE(span.length == 7);
}

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
