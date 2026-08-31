#pragma once

#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

class RepeatMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::REPEAT;

public:
	explicit RepeatMatcher(Matcher &element_p) : Matcher(TYPE), element(element_p) {
	}

	MatchResultType Match(MatchState &state) const override {
		MatchState repeat_state(state);

		// first we must match the element
		auto child_match = element.Match(repeat_state);
		if (child_match != MatchResultType::SUCCESS) {
			// match did not succeed - propagate upwards
			return child_match;
		}
		// we have matched (at least) once - so this is always a success
		// now we can keep on repeating the matching (optionally)
		while (true) {
			// update the token index we propagate upwards
			state.token_iterator.SetPosition(repeat_state.token_iterator);

			auto current = repeat_state.token_iterator.Current();
			bool at_autocomplete_cursor = current && current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE;
			if (at_autocomplete_cursor) {
				element.AddSuggestion(state);
				return MatchResultType::SUCCESS;
			}

			// now match the element again
			child_match = element.Match(repeat_state);
			if (child_match != MatchResultType::SUCCESS) {
				// if we did not succeed we are done matching
				return MatchResultType::SUCCESS;
			}
		}
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		MatchState repeat_state(state);
		vector<reference<ParseResult>> results;

		optional_idx start_offset;
		if (auto current = repeat_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}

		// First, we MUST match the element at least once.
		auto first_result = element.MatchParseResult(repeat_state);
		if (!first_result) {
			// The first match failed, so the whole repeat fails.
			return nullptr;
		}
		results.push_back(*first_result);

		// After the first success, the overall result is a success.
		// Now, we continue matching the element as many times as possible.
		while (true) {
			// Propagate the new state upwards.
			state.token_iterator.SetPosition(repeat_state.token_iterator);

			auto current = repeat_state.token_iterator.Current();
			if (current && current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
				break;
			}

			// Try to match the element again.
			auto next_result = element.MatchParseResult(repeat_state);
			if (!next_result) {
				break;
			}
			results.push_back(*next_result);
		}

		// Return all collected results in a RepeatParseResult.
		return state.allocator.Allocate(make_uniq<RepeatParseResult>(std::move(results), start_offset));
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		element.AddSuggestion(state);
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return element.GetName() + "*";
	}

private:
	Matcher &element;
};

} // namespace duckdb
