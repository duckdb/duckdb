#pragma once

#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

//! Consumes the END_OF_INPUT sentinel; wired into the grammar's EndOfInput rule.
class EndOfInputMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::END_OF_INPUT;

public:
	EndOfInputMatcher() : Matcher(TYPE) {
	}

	MatchResultType Match(MatchState &state) const override {
		if (state.token_index < state.tokens.size() &&
		    state.tokens[state.token_index].type == TokenType::END_OF_INPUT) {
			state.token_index++;
			state.UpdateMaxTokenIndex();
			return MatchResultType::SUCCESS;
		}
		return MatchResultType::FAIL;
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		if (state.token_index < state.tokens.size() &&
		    state.tokens[state.token_index].type == TokenType::END_OF_INPUT) {
			state.token_index++;
			state.UpdateMaxTokenIndex();
			return state.allocator.Allocate(make_uniq<EndOfInputParseResult>());
		}
		return nullptr;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "EndOfInput";
	}
};

} // namespace duckdb
