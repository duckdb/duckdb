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
		while (IsStringLiteralContinuation(state)) {
			Advance(state);
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
		auto string_info = GetSpecialStringInfo(token->text);

		if (!IsStringLiteral(*token, string_info)) {
			return nullptr;
		}
		string stripped_string = StripStringLiteral(*token, string_info);
		idx_t end_offset = token->offset + token->length;
		Advance(state);
		while (IsStringLiteralContinuation(state)) {
			token = state.token_iterator.Current();
			auto continuation_info = GetSpecialStringInfo(token->text);
			stripped_string += StripStringLiteral(*token, continuation_info);
			end_offset = token->offset + token->length;
			Advance(state);
		}
		auto token_length = optional_idx(end_offset - start_offset.GetIndex());

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
	static bool IsStringLiteral(const MatcherToken &token, const SpecialStringInfo &string_info) {
		if (string_info.prefix_len == 0) {
			return false;
		}
		const idx_t minimum_literal_length = string_info.prefix_len + 1;
		if (token.text.size() < minimum_literal_length) {
			return false;
		}
		const idx_t opening_quote_position = string_info.prefix_len - 1;
		if (token.text[opening_quote_position] != '\'') {
			return false;
		}
		return token.text.back() == '\'';
	}

	static string StripStringLiteral(const MatcherToken &token, const SpecialStringInfo &string_info) {
		idx_t suffix_len = 1;
		auto stripped_string =
		    token.text.substr(string_info.prefix_len, token.text.length() - (string_info.prefix_len + suffix_len));
		return StringUtil::Replace(stripped_string, "''", "'");
	}

	static void Advance(MatchState &state) {
		state.token_iterator.Advance();
		state.UpdateMaxTokenIndex();
	}

	static bool IsStringLiteralContinuation(MatchState &state) {
		auto token = state.token_iterator.Current();
		if (!token) {
			return false;
		}
		if (!token->preceded_by_newline) {
			return false;
		}
		if (token->preceded_by_block_comment) {
			return false;
		}
		if (token->type != TokenType::STRING_LITERAL) {
			return false;
		}
		auto string_info = GetSpecialStringInfo(token->text);
		if (string_info.type != SpecialStringCharacter::STANDARD) {
			return false;
		}
		return IsStringLiteral(*token, string_info);
	}

	static bool MatchStringLiteral(MatchState &state, const SpecialStringInfo &string_info) {
		auto token = state.token_iterator.Current();
		if (!token) {
			return false;
		}
		if (IsStringLiteral(*token, string_info)) {
			Advance(state);
			return true;
		}
		return false;
	}
};

} // namespace duckdb
