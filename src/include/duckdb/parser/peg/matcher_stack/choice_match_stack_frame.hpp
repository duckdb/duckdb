#pragma once

#include "duckdb/parser/peg/matcher_stack.hpp"
#include "duckdb/parser/peg/matcher/choice_matcher.hpp"

namespace duckdb {

class ChoiceMatchStackFrame : public MatchStackFrame {
public:
	ChoiceMatchStackFrame(match_frame_index_t frame_index, const ChoiceMatcher &matcher, MatchState &state)
	    : MatchStackFrame(frame_index, matcher, state), choice_matcher(matcher) {
		if (auto current = state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
	}

	void Execute(MatchStack &stack) override {
		if (HasChildResult()) {
			auto child_result = TakeChildResult();
			if (!child_state) {
				throw InternalException("Choice frame has a child result but no child state");
			}
			if (child_result.IsSuccess()) {
				match_state.token_iterator.SetPosition(child_state->token_iterator);
				if (!child_result.HasParseResult()) {
					SetResult(MatcherResult::Success());
					return;
				}
				SetResult(match_state.AllocateParseResult<ChoiceParseResult>(*child_result.GetParseResult(),
				                                                             child_index, start_offset));
				return;
			}
			child_index++;
			child_state.reset();
		}
		while (child_index < choice_matcher.matchers.size() &&
		       !choice_matcher.matchers[child_index].get().CanStartAt(match_state)) {
			child_index++;
		}
		if (child_index >= choice_matcher.matchers.size()) {
			SetResult(MatcherResult::Failure());
			return;
		}
		child_state.emplace(match_state);
		stack.PushChildFrame(*this, choice_matcher.matchers[child_index].get(), *child_state);
	}

private:
	const ChoiceMatcher &choice_matcher;
	optional<MatchState> child_state;
	idx_t child_index = 0;
	optional_idx start_offset;
};

} // namespace duckdb
