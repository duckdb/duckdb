#pragma once

#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

class ArithmeticOperatorMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::OPERATOR;

public:
	explicit ArithmeticOperatorMatcher() : Matcher(TYPE) {
	}

	MatchResultType Match(MatchState &state) const override {
		if (!MatchArithmeticOperator(state)) {
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
		if (!MatchArithmeticOperator(state)) {
			return nullptr;
		}
		return state.allocator.Allocate(make_uniq<OperatorParseResult>(token_text, start_offset, token_length));
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "ARITHMETICOPERATOR";
	}

private:
	bool MatchArithmeticOperator(MatchState &state) const {
		if (state.token_index >= state.tokens.size()) {
			return false;
		}
		auto &token_text = state.tokens[state.token_index].text;
		for (auto &c : token_text) {
			if (!IsArithmeticOperatorChar(c)) {
				return false;
			}
		}
		state.token_index++;
		state.UpdateMaxTokenIndex();
		return true;
	}

private:
	bool IsArithmeticOperatorChar(char c) const {
		switch (c) {
		case '+':
		case '-':
		case '*':
		case '/':
		case '%':
			return true;
		default:
			return false;
		}
	}
};

} // namespace duckdb
