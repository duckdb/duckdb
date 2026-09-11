#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: Connection::Tokenize. The tokenizer's behavior is pinned
// by the C API tests; these cover what the C++ layer adds on top.
// ---------------------------------------------------------------------------

namespace {

using duckdb::cxx::Token;
using duckdb::cxx::TokenType;

// Token has no operator==; compare field-wise and report the first mismatch.
void RequireSameTokens(const std::vector<Token> &actual, const std::vector<Token> &expected) {
	REQUIRE(actual.size() == expected.size());
	for (size_t i = 0; i < expected.size(); i++) {
		INFO("token " << i);
		REQUIRE(static_cast<int>(actual[i].type) == static_cast<int>(expected[i].type));
		REQUIRE(actual[i].start == expected[i].start);
		REQUIRE(actual[i].length == expected[i].length);
	}
}

} // namespace

TEST_CASE("Stable C++API: Tokenize returns typed byte ranges", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::string sql = "SELECT \"my col\", 'it''s', $$d$$, 1e5, a <> b -- c\n/* d */;";
	std::vector<Token> expected {
	    {TokenType::KEYWORD, 0, 6},         {TokenType::IDENTIFIER, 7, 8},      {TokenType::OPERATOR, 15, 1},
	    {TokenType::STRING_LITERAL, 17, 7}, {TokenType::OPERATOR, 24, 1},       {TokenType::STRING_LITERAL, 26, 5},
	    {TokenType::OPERATOR, 31, 1},       {TokenType::NUMBER_LITERAL, 33, 3}, {TokenType::OPERATOR, 36, 1},
	    {TokenType::IDENTIFIER, 38, 1},     {TokenType::OPERATOR, 40, 2},       {TokenType::IDENTIFIER, 43, 1},
	    {TokenType::COMMENT, 45, 5},        {TokenType::COMMENT, 50, 7},        {TokenType::TERMINATOR, 57, 1},
	};
	auto tokens = conn.Tokenize(sql);
	RequireSameTokens(tokens, expected);

	// Every range slices a lexeme out of the input, and none reaches past its end.
	REQUIRE(sql.substr(tokens[1].start, tokens[1].length) == "\"my col\"");
	REQUIRE(sql.substr(tokens[3].start, tokens[3].length) == "'it''s'");
	REQUIRE(tokens.back().start + tokens.back().length == sql.size());
}

TEST_CASE("Stable C++API: Tokenize honors the view's length, not a terminator", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// A view that stops before the terminator, and one that carries an interior NUL.
	std::string sql = "SELECT 1; SELECT 2";
	RequireSameTokens(conn.Tokenize(std::string_view(sql).substr(0, 9)),
	                  {{TokenType::KEYWORD, 0, 6}, {TokenType::NUMBER_LITERAL, 7, 1}, {TokenType::TERMINATOR, 8, 1}});
	std::string with_nul("1\0 2", 4);
	RequireSameTokens(
	    conn.Tokenize(with_nul),
	    {{TokenType::NUMBER_LITERAL, 0, 1}, {TokenType::IDENTIFIER, 1, 1}, {TokenType::NUMBER_LITERAL, 3, 1}});
}

TEST_CASE("Stable C++API: Tokenize on empty and malformed input", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	REQUIRE(conn.Tokenize("").empty());
	REQUIRE(conn.Tokenize(" \n\t").empty());

	// An unterminated string does not throw; its token runs to the end of the input.
	RequireSameTokens(conn.Tokenize("SELECT 'abc"), {{TokenType::KEYWORD, 0, 6}, {TokenType::STRING_LITERAL, 7, 4}});
}

TEST_CASE("Stable C++API: Tokenize on a moved-from connection throws", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto other = std::move(conn);
	REQUIRE_THROWS_MATCHES(conn.Tokenize("SELECT 1"), InvalidInputException, // NOLINT: pinning the moved-from state
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	RequireSameTokens(other.Tokenize("SELECT 1"), {{TokenType::KEYWORD, 0, 6}, {TokenType::NUMBER_LITERAL, 7, 1}});
}
