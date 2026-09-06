#pragma once

#include "duckdb/parser/peg/matcher_stack.hpp"
#include "duckdb/parser/peg/matcher/list_matcher.hpp"

namespace duckdb {

class ListMatchStackFrame : public MatchStackFrame {
public:
	ListMatchStackFrame(match_frame_index_t frame_index, const ListMatcher &matcher, MatchState &state)
	    : MatchStackFrame(frame_index, matcher, state), list_matcher(matcher), list_state(state) {
		saved_suggestion_size = matcher.suppress_suggestions ? list_state.suggestions.size() : 0;
		if (auto current = list_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
	}

	void Execute(MatchStack &stack) override {
		if (HasChildResult()) {
			auto child_result = TakeChildResult();
			if (!child_result.IsSuccess()) {
				DiscardSuggestions();
				SetResult(MatcherResult::Failure());
				return;
			}
			if (child_result.HasParseResult()) {
				results.push_back(*child_result.GetParseResult());
			}
			child_index++;
		}
		while (child_index < list_matcher.matchers.size()) {
			auto current = list_state.token_iterator.Current();
			bool at_autocomplete_cursor = current && current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE;
			if (!at_autocomplete_cursor) {
				stack.PushChildFrame(*this, list_matcher.matchers[child_index].get(), list_state);
				return;
			}
			if (list_matcher.suppress_suggestions) {
				DiscardSuggestions();
				SetResult(MatcherResult::Failure());
				return;
			}
			if (list_matcher.matchers[child_index].get().AddSuggestion(list_state) == SuggestionType::OPTIONAL) {
				child_index++;
				continue;
			}
			match_state.token_iterator.SetPosition(list_state.token_iterator);
			SetResult(MatcherResult::Failure());
			return;
		}
		match_state.token_iterator.SetPosition(list_state.token_iterator);
		DiscardSuggestions();
		auto list_name = list_matcher.HasName() ? list_matcher.GetName() : string();
		SetResult(
		    match_state.AllocateParseResult<ListParseResult>(std::move(results), std::move(list_name), start_offset));
	}

private:
	void DiscardSuggestions() {
		if (!list_matcher.suppress_suggestions) {
			return;
		}
		list_state.suggestions.erase(list_state.suggestions.begin() + NumericCast<int64_t>(saved_suggestion_size),
		                             list_state.suggestions.end());
	}

private:
	const ListMatcher &list_matcher;
	MatchState list_state;
	vector<reference<ParseResult>> results;
	idx_t child_index = 0;
	idx_t saved_suggestion_size = 0;
	optional_idx start_offset;
};

} // namespace duckdb
