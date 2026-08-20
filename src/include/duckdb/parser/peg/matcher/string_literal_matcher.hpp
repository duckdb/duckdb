#pragma once

#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

class StringLiteralMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::STRING_LITERAL;

public:
	explicit StringLiteralMatcher() : Matcher(TYPE) {
		name = "StringLiteral";
	}

	MatchResultType Match(MatchState &state) const override {
		auto token = state.token_iterator.Current();
		if (!token) {
			return MatchResultType::FAIL;
		}

		auto &token_text = token->text;
		auto string_info = GetSpecialStringInfo(token_text);

		if (!MatchStringLiteral(state, string_info)) {
			return MatchResultType::FAIL;
		}
		state.token_iterator.SetPreviousTokenType(TokenType::STRING_LITERAL);
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		auto token = state.token_iterator.Current();
		if (!token) {
			return nullptr;
		}

		auto start_offset = optional_idx(token->offset);
		auto token_length = optional_idx(token->length);
		auto string_info = GetSpecialStringInfo(token->text);

		if (!MatchStringLiteral(state, string_info)) {
			return nullptr;
		}

		idx_t suffix_len = 1;
		if (token->text.length() < string_info.prefix_len + suffix_len) {
			return nullptr;
		}

		string stripped_string =
		    token->text.substr(string_info.prefix_len, token->text.length() - (string_info.prefix_len + suffix_len));
		stripped_string = StringUtil::Replace(stripped_string, "''", "'");

		auto result = state.allocator.Allocate(
		    make_uniq<StringLiteralParseResult>(stripped_string, string_info.type, start_offset, token_length));
		result->name = name;
		return result;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "STRING_LITERAL";
	}

private:
	static bool MatchStringLiteral(MatchState &state, const SpecialStringInfo &string_info) {
		auto token = state.token_iterator.Current();
		if (!token) {
			return false;
		}
		auto &token_text = token->text;

		idx_t open_quote_idx = string_info.prefix_len - 1;
		idx_t min_len = string_info.prefix_len + 1;

		if (token_text.size() >= min_len && token_text[open_quote_idx] == '\'' && token_text.back() == '\'') {
			state.token_iterator.Advance();
			state.UpdateMaxTokenIndex();
			return true;
		}
		return false;
	}
};

} // namespace duckdb
