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

	MatcherResult MatchParseResultInternal(MatchState &state) const override {
		MatchState list_state(state);
		auto &allocator = state.allocator;
		auto children_begin = allocator.ChildrenBegin();
		// when suppress_suggestions is set, we discard any suggestions added by child matchers
		auto saved_suggestion_size = suppress_suggestions ? list_state.suggestions.size() : 0;

		optional_idx start_offset;
		if (auto current = list_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
		for (auto &child_matcher : matchers) {
			auto current = list_state.token_iterator.Current();
			bool at_autocomplete_cursor = current && current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE;
			if (!at_autocomplete_cursor) {
				auto child_result = child_matcher.get().MatchParseResult(list_state);
				if (!child_result.IsSuccess()) {
					DiscardSuggestions(list_state, saved_suggestion_size);
					allocator.DiscardChildren(children_begin);
					return MatcherResult::Failure();
				}
				if (child_result.HasParseResult()) {
					allocator.PushChild(*child_result.GetParseResult());
				}
				continue;
			}
			if (suppress_suggestions) {
				DiscardSuggestions(list_state, saved_suggestion_size);
				allocator.DiscardChildren(children_begin);
				return MatcherResult::Failure();
			}
			if (child_matcher.get().AddSuggestion(list_state) == SuggestionType::OPTIONAL) {
				continue;
			}
			state.token_iterator.SetPosition(list_state.token_iterator);
			allocator.DiscardChildren(children_begin);
			return MatcherResult::Failure();
		}
		state.token_iterator.SetPosition(list_state.token_iterator);
		DiscardSuggestions(list_state, saved_suggestion_size);
		idx_t child_count;
		auto children = allocator.TakeChildren(children_begin, child_count);
		return state.AllocateParseResult<ListParseResult>(children, child_count, start_offset);
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

private:
	void DiscardSuggestions(MatchState &state, idx_t saved_suggestion_size) const {
		if (!suppress_suggestions) {
			return;
		}
		state.suggestions.erase(state.suggestions.begin() + NumericCast<int64_t>(saved_suggestion_size),
		                        state.suggestions.end());
	}

public:
	vector<reference<Matcher>> matchers;
	//! If true, this matcher will not contribute autocomplete suggestions (used for rules like ExpressionStatement)
	bool suppress_suggestions = false;
};

} // namespace duckdb
