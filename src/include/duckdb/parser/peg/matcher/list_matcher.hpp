#pragma once

#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

class ListMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::LIST;

public:
	ListMatcher() : Matcher(TYPE) {
	}
	explicit ListMatcher(vector<reference<Matcher>> matchers_p) : Matcher(TYPE), matchers(std::move(matchers_p)) {
	}

	MatchResultType Match(MatchState &state) const override {
		MatchState list_state(state);
		// when suppress_suggestions is set, we discard any suggestions added by child matchers
		auto saved_suggestion_size = suppress_suggestions ? list_state.suggestions.size() : 0;
		for (idx_t child_idx = 0; child_idx < matchers.size(); child_idx++) {
			auto &child_matcher = matchers[child_idx].get();
			auto current = list_state.token_iterator.Current();
			bool at_autocomplete_cursor = current && current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE;
			if (at_autocomplete_cursor) {
				if (suppress_suggestions) {
					// this rule should not contribute autocomplete suggestions
					// discard any suggestions added by earlier children
					list_state.suggestions.erase(list_state.suggestions.begin() +
					                                 NumericCast<int64_t>(saved_suggestion_size),
					                             list_state.suggestions.end());
					return MatchResultType::FAIL;
				}
				// cursor is here - push suggestions for what could follow
				for (; child_idx < matchers.size(); child_idx++) {
					auto suggestion_type = matchers[child_idx].get().AddSuggestion(list_state);
					if (suggestion_type == SuggestionType::MANDATORY) {
						// finished providing suggestions
						break;
					}
				}
				state.token_iterator.SetPosition(list_state.token_iterator);
				if (child_idx == matchers.size()) {
					// we managed to provide suggestions for all tokens
					// that means all other tokens were optional - i.e. we succeeded in matching them
					return MatchResultType::SUCCESS;
				}
				return MatchResultType::FAIL;
			}
			auto match_result = child_matcher.Match(list_state);
			if (match_result != MatchResultType::SUCCESS) {
				if (suppress_suggestions) {
					list_state.suggestions.erase(list_state.suggestions.begin() +
					                                 NumericCast<int64_t>(saved_suggestion_size),
					                             list_state.suggestions.end());
				}
				return match_result;
			}
		}
		// we matched all child matchers - propagate token index upward
		state.token_iterator.SetPosition(list_state.token_iterator);
		if (suppress_suggestions) {
			// discard suggestions from child matchers that were added during matching
			state.suggestions.erase(state.suggestions.begin() + NumericCast<int64_t>(saved_suggestion_size),
			                        state.suggestions.end());
		}
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		MatchState list_state(state);
		vector<reference<ParseResult>> results;

		optional_idx start_offset;
		if (auto current = list_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
		for (const auto &child_matcher : matchers) {
			auto child_result = child_matcher.get().MatchParseResult(list_state);
			if (!child_result) {
				return nullptr;
			}
			results.push_back(*child_result);
		}
		state.token_iterator.SetPosition(list_state.token_iterator);
		// Empty name implies it's a subrule, e.g. 'SET'i (StandardAssignment / SetTimeZone)
		return state.allocator.Allocate(make_uniq<ListParseResult>(std::move(results), name, start_offset));
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		if (suppress_suggestions) {
			return SuggestionType::OPTIONAL;
		}
		for (auto &matcher : matchers) {
			auto suggestion_result = matcher.get().AddSuggestion(state);
			if (suggestion_result == SuggestionType::MANDATORY) {
				// we must match this suggestion before continuing
				return SuggestionType::MANDATORY;
			}
		}
		// all child suggestions were optional - the entire list is optional
		return SuggestionType::OPTIONAL;
	}

	string ToString() const override {
		string result = "";
		for (auto &matcher : matchers) {
			if (!result.empty()) {
				result += " ";
			}
			result += matcher.get().GetName();
		}
		return "(" + result + ")";
	}

public:
	vector<reference<Matcher>> matchers;
	//! If true, this matcher will not contribute autocomplete suggestions (used for rules like ExpressionStatement)
	bool suppress_suggestions = false;
};

} // namespace duckdb
