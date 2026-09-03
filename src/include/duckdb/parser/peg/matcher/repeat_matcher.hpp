#pragma once

#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

class RepeatMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::REPEAT;

public:
	explicit RepeatMatcher(Matcher &element_p) : Matcher(TYPE), element(element_p) {
	}

	MatcherResult MatchParseResultInternal(MatchState &state) const override {
		MatchState repeat_state(state);
		vector<reference<ParseResult>> results;

		optional_idx start_offset;
		if (auto current = repeat_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}

		// First, we MUST match the element at least once.
		auto first_result = element.MatchParseResult(repeat_state);
		if (!first_result.IsSuccess()) {
			// The first match failed, so the whole repeat fails.
			return MatcherResult::Failure();
		}
		if (first_result.HasParseResult()) {
			results.push_back(*first_result.GetParseResult());
		}

		// After the first success, the overall result is a success.
		// Now, we continue matching the element as many times as possible.
		while (true) {
			// Propagate the new state upwards.
			state.token_iterator.SetPosition(repeat_state.token_iterator);

			auto current = repeat_state.token_iterator.Current();
			if (current && current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
				element.AddSuggestion(state);
				break;
			}

			// Try to match the element again.
			auto next_result = element.MatchParseResult(repeat_state);
			if (!next_result.IsSuccess()) {
				break;
			}
			if (next_result.HasParseResult()) {
				results.push_back(*next_result.GetParseResult());
			}
		}

		// Return all collected results in a RepeatParseResult.
		return state.AllocateParseResult<RepeatParseResult>(std::move(results), start_offset);
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		element.AddSuggestion(state);
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return element.GetName() + "*";
	}
	const Matcher &GetChildMatcher() const {
		return element;
	}

private:
	Matcher &element;
};

} // namespace duckdb
