#pragma once

#include "duckdb/parser/peg/matcher_stack.hpp"
#include "duckdb/parser/peg/matcher/repeat_matcher.hpp"

namespace duckdb {

class RepeatMatchStackFrame : public MatchStackFrame {
public:
	RepeatMatchStackFrame(match_frame_index_t frame_index, const RepeatMatcher &matcher, MatchState &state)
	    : MatchStackFrame(frame_index, matcher, state), repeat_matcher(matcher), repeat_state(state) {
		children_begin = state.allocator.ChildrenBegin();
		if (auto current = repeat_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
	}

	void Execute(MatchStack &stack) override {
		if (HasChildResult()) {
			auto child_result = TakeChildResult();
			if (!child_result.IsSuccess()) {
				if (!matched_once) {
					match_state.allocator.DiscardChildren(children_begin);
					SetResult(MatcherResult::Failure());
				} else {
					SetRepeatResult();
				}
				return;
			}
			matched_once = true;
			if (child_result.HasParseResult()) {
				match_state.allocator.PushChild(*child_result.GetParseResult());
			}
			match_state.token_iterator.SetPosition(repeat_state.token_iterator);
			auto current = repeat_state.token_iterator.Current();
			if (current && current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
				repeat_matcher.GetChildMatcher().AddSuggestion(match_state);
				SetRepeatResult();
				return;
			}
		}
		stack.PushChildFrame(*this, repeat_matcher.GetChildMatcher(), repeat_state);
	}

private:
	void SetRepeatResult() {
		idx_t child_count;
		auto children = match_state.allocator.TakeChildren(children_begin, child_count);
		SetResult(match_state.AllocateParseResult<RepeatParseResult>(children, child_count, start_offset));
	}

private:
	const RepeatMatcher &repeat_matcher;
	MatchState repeat_state;
	idx_t children_begin;
	bool matched_once = false;
	optional_idx start_offset;
};

} // namespace duckdb
