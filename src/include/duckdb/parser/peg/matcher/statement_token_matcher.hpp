#pragma once

#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

//! Consumes any single token that is not a statement boundary, without interpreting it. Repeating this matcher
//! covers a statement whose syntax this parser does not know, e.g. SQL destined for a remote engine.
class StatementTokenMatcher : public AtomicMatcher {
public:
	static constexpr MatcherType TYPE = MatcherType::CUSTOM;

public:
	StatementTokenMatcher() : AtomicMatcher(TYPE) {
	}

	MatcherResult MatchAtomic(MatchState &state) const override {
		auto current = state.token_iterator.Current();
		if (!current || current->type == TokenType::TERMINATOR || current->type == TokenType::END_OF_INPUT ||
		    current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
			return MatcherResult::Failure();
		}
		// always advances, so a repeat over this matcher terminates
		auto text = current->text;
		auto offset = current->offset;
		auto length = current->length;
		state.token_iterator.Advance();
		state.UpdateMaxTokenIndex();
		return state.AllocateParseResult<TokenParseResult>(std::move(text), offset, length);
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		// any token matches - there is nothing to suggest
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "StatementToken";
	}
};

} // namespace duckdb
