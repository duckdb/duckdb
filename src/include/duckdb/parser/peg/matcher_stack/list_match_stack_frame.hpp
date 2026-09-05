#pragma once

#include "duckdb/parser/peg/matcher_stack.hpp"
#include "duckdb/parser/peg/matcher/list_matcher.hpp"

namespace duckdb {

class ListMatchStackFrame : public MatchStackFrame {
public:
	ListMatchStackFrame(match_frame_index_t frame_index, const ListMatcher &matcher, MatchState &state)
	    : MatchStackFrame(frame_index, matcher, state), list_matcher(matcher), list_state(state) {
		children_begin = state.allocator.ChildrenBegin();
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
				match_state.allocator.DiscardChildren(children_begin);
				SetResult(MatcherResult::Failure());
				return;
			}
			if (child_result.HasParseResult()) {
				match_state.allocator.PushChild(*child_result.GetParseResult());
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
				match_state.allocator.DiscardChildren(children_begin);
				SetResult(MatcherResult::Failure());
				return;
			}
			if (list_matcher.matchers[child_index].get().AddSuggestion(list_state) == SuggestionType::OPTIONAL) {
				child_index++;
				continue;
			}
			match_state.token_iterator.SetPosition(list_state.token_iterator);
			match_state.allocator.DiscardChildren(children_begin);
			SetResult(MatcherResult::Failure());
			return;
		}
		match_state.token_iterator.SetPosition(list_state.token_iterator);
		DiscardSuggestions();
		idx_t child_count;
		auto children = match_state.allocator.TakeChildren(children_begin, child_count);
		SetResult(match_state.AllocateParseResult<ListParseResult>(children, child_count, start_offset));
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
	idx_t children_begin;
	idx_t child_index = 0;
	idx_t saved_suggestion_size = 0;
	optional_idx start_offset;
};

} // namespace duckdb
