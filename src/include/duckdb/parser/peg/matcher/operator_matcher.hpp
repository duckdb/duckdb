#pragma once

#include "duckdb/parser/peg/tokenizer/base_tokenizer.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

static bool IsOperatorChar(char c) {
	switch (c) {
	case '+':
	case '-':
	case '*':
	case '/':
	case '%':
	case '^':
	case '<':
	case '>':
	case '=':
	case '~':
	case '!':
	case '@':
	case '&':
	case '|':
		return true;
	default:
		return false;
	}
}

class OperatorMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::OPERATOR;

public:
	explicit OperatorMatcher() : Matcher(TYPE) {
	}

	MatchResultType Match(MatchState &state) const override {
		if (!MatchOperator(state)) {
			return MatchResultType::FAIL;
		}
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		if (state.token_index >= state.tokens.size()) {
			return nullptr;
		}
		auto &token_text = state.tokens[state.token_index].text;
		auto start_offset = optional_idx(state.tokens[state.token_index].offset);
		auto token_length = optional_idx(state.tokens[state.token_index].length);
		if (!MatchOperator(state)) {
			return nullptr;
		}
		return state.allocator.Allocate(make_uniq<OperatorParseResult>(token_text, start_offset, token_length));
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "OPERATOR";
	}

private:
	static bool MatchOperator(MatchState &state) {
		if (state.token_index >= state.tokens.size()) {
			return false;
		}
		auto &token_text = state.tokens[state.token_index].text;
		// Exclude the lambda arrow and JSON arrow — these have dedicated grammar roles
		if (token_text == "->" || token_text == "->>") {
			return false;
		}
		// Single-character operators are handled at specific precedence levels (comparison, additive, etc.)
		if (token_text.size() == 1) {
			return false;
		}
		// Exclude known comparison operators — handled by ComparisonExpression, not as function calls
		if (token_text == "<=" || token_text == ">=" || token_text == "!=" || token_text == "==" ||
		    token_text == "<>") {
			return false;
		}
		// Exclude LIKE/SIMILAR operators — handled by LikeVariations at a higher precedence level
		if (token_text == "~~" || token_text == "~~*" || token_text == "~~~" || token_text == "~*" ||
		    token_text == "!~~" || token_text == "!~~*" || token_text == "!~" || token_text == "!~*") {
			return false;
		}
		for (auto &c : token_text) {
			if (!IsOperatorChar(c)) {
				return false;
			}
		}
		state.token_index++;
		state.UpdateMaxTokenIndex();
		return true;
	}
};

} // namespace duckdb
