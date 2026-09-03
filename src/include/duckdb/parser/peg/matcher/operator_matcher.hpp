#pragma once

#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

class OperatorMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::OPERATOR;

public:
	explicit OperatorMatcher() : Matcher(TYPE) {
	}

	MatcherResult MatchParseResultInternal(MatchState &state) const override {
		auto token = state.token_iterator.Current();
		if (!token) {
			return MatcherResult::Failure();
		}
		auto &token_text = token->text;
		auto start_offset = optional_idx(token->offset);
		auto token_length = optional_idx(token->length);
		if (!MatchOperator(state)) {
			return MatcherResult::Failure();
		}
		return state.AllocateParseResult<OperatorParseResult>(token_text, start_offset, token_length);
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "OPERATOR";
	}

private:
	bool MatchOperator(MatchState &state) const {
		auto token = state.token_iterator.Current();
		if (!token) {
			return false;
		}
		auto &token_text = token->text;
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
		state.token_iterator.Advance();
		state.UpdateMaxTokenIndex();
		return true;
	}

protected:
	bool IsOperatorChar(char c) const {
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
};

} // namespace duckdb
