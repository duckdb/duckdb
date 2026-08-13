#pragma once

#include "duckdb/parser/peg/tokenizer/base_tokenizer.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

class NumberLiteralMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::NUMBER_LITERAL;

public:
	explicit NumberLiteralMatcher() : Matcher(TYPE) {
		name = "NumberLiteral";
	}

	MatchResultType Match(MatchState &state) const override {
		// variable matchers match anything except for reserved keywords
		if (!MatchNumberLiteral(state)) {
			return MatchResultType::FAIL;
		}
		state.tokens[state.token_index - 1].type = TokenType::NUMBER_LITERAL;
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		if (state.token_index >= state.tokens.size()) {
			return nullptr;
		}
		auto &token_text = state.tokens[state.token_index].text;
		auto start_offset = optional_idx(state.tokens[state.token_index].offset);
		auto token_length = optional_idx(state.tokens[state.token_index].length);
		if (!MatchNumberLiteral(state)) {
			return nullptr;
		}
		auto result = state.allocator.Allocate(make_uniq<NumberParseResult>(token_text, start_offset, token_length));
		result->name = name;
		return result;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "NUMBER_LITERAL";
	}

private:
	static bool MatchNumberLiteral(MatchState &state) {
		if (state.token_index >= state.tokens.size()) {
			return false;
		}
		auto &token_text = state.tokens[state.token_index].text;
		if (token_text.empty() || !BaseTokenizer::CharacterIsInitialNumber(token_text[0])) {
			return false;
		}
		// A lone '.' is a dot operator, not a number literal (e.g., '?.method()' should not consume '.')
		if (token_text.size() == 1 && token_text[0] == '.') {
			return false;
		}
		bool scientific_notation = false;
		for (idx_t i = 1; i < token_text.size(); i++) {
			if (BaseTokenizer::CharacterIsScientific(token_text[i])) {
				if (scientific_notation) {
					throw ParserException("Already found scientific notation");
				}
				scientific_notation = true;
			}
			if (scientific_notation && (token_text[i] == '+' || token_text[i] == '-')) {
				continue;
			}
			if (!BaseTokenizer::CharacterIsNumber(token_text[i])) {
				return false;
			}
		}
		state.token_index++;
		state.UpdateMaxTokenIndex();
		return true;
	}
};

} // namespace duckdb
