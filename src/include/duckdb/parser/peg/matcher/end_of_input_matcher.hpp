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

	MatcherResult MatchParseResultInternal(MatchState &state) const override {
		auto current = state.token_iterator.Current();
		if (current && current->type == TokenType::END_OF_INPUT) {
			state.token_iterator.Advance();
			state.UpdateMaxTokenIndex();
			return state.AllocateParseResult<EndOfInputParseResult>();
		}
		return MatcherResult::Failure();
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "EndOfInput";
	}
};

} // namespace duckdb
