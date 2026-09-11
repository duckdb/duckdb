#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/parser/peg/dialect_extension.hpp"
#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"

using namespace duckdb;

class EmptyKeywordHelper final : public PEGKeywordHelper {
public:
	bool KeywordCategoryType(const string &, PEGKeywordCategory) const override {
		return false;
	}
	bool IsKeyword(const string &) const override {
		return false;
	}
	vector<ParserKeyword> KeywordList() const override {
		return {};
	}
};

class HookTokenizer final : public Tokenizer {
public:
	HookTokenizer(const PEGKeywordHelper &helper, bool strings, bool backticks, bool split_operators)
	    : Tokenizer(helper), strings(strings), backticks(backticks), split_operators(split_operators) {
	}

protected:
	bool BackslashEscapesStringLiterals() const override {
		return strings;
	}
	bool IsQuotedIdentifierDelimiter(char character) const override {
		return character == '"' || (backticks && character == '`');
	}
	void PushOperatorToken(TokenizerBehavior &behavior, idx_t start, idx_t end) const override {
		auto &sql = behavior.sql;
		if (!split_operators || end - start < 2 || sql[start] != '>') {
			Tokenizer::PushOperatorToken(behavior, start, end);
			return;
		}
		behavior.PushToken(start, start + 1, TokenType::OPERATOR);
		behavior.PushToken(start + 1, end, TokenType::OPERATOR);
	}

private:
	bool strings;
	bool backticks;
	bool split_operators;
};

static vector<MatcherToken> Tokenize(const Tokenizer &tokenizer, const string &sql) {
	vector<MatcherToken> tokens;
	TokenizerBehavior behavior(sql, tokens);
	tokenizer.TokenizeInput(behavior);
	return tokens;
}

TEST_CASE("Register and select a dialect extension", "[api][dialect_extension]") {
	DBConfig config;
	DialectExtension::Register(config, make_uniq<DialectExtension>("test"));

	DuckDB db(nullptr, &config);
	Connection con(db);

	auto dialects = con.Query("SELECT dialect_name FROM duckdb_dialects() ORDER BY dialect_name");
	REQUIRE_NO_FAIL(*dialects);
	REQUIRE(dialects->RowCount() == 1);
	REQUIRE(dialects->GetValue(0, 0) == Value("test"));

	REQUIRE_NO_FAIL(con.Query("SET current_dialect = 'test'"));
	auto current_dialect = con.Query("SELECT current_setting('current_dialect')");
	REQUIRE_NO_FAIL(*current_dialect);
	REQUIRE(current_dialect->GetValue(0, 0) == Value("test"));
}

TEST_CASE("Dialect tokenizer hooks are opt-in", "[api][dialect_extension]") {
	EmptyKeywordHelper helper;
	Tokenizer default_tokenizer(helper);
	HookTokenizer hook_tokenizer(helper, true, true, true);

	auto default_backtick = Tokenize(default_tokenizer, "`a b`");
	auto hooked_backtick = Tokenize(hook_tokenizer, "`a b`");
	REQUIRE(default_backtick.size() > 2);
	REQUIRE(hooked_backtick.size() >= 1);
	REQUIRE(hooked_backtick[0].type == TokenType::IDENTIFIER);
	REQUIRE(hooked_backtick[0].text == "`a b`");

	auto default_operator = Tokenize(default_tokenizer, ">>=");
	auto hooked_operator = Tokenize(hook_tokenizer, ">>=");
	REQUIRE(default_operator.size() >= 1);
	REQUIRE(default_operator[0].text == ">>=");
	REQUIRE(default_operator.size() >= 2);
	REQUIRE(hooked_operator[0].text == ">");
	REQUIRE(hooked_operator[1].text == ">=");

	auto default_string = Tokenize(default_tokenizer, "'a\\'b'");
	auto hooked_string = Tokenize(hook_tokenizer, "'a\\'b'");
	REQUIRE(default_string.size() > 2);
	REQUIRE(hooked_string[0].text == "'a\\'b'");
}
