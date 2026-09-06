#pragma once

#include "duckdb/parser/peg/matcher_stack.hpp"
#include "duckdb/parser/peg/matcher/optional_matcher.hpp"

namespace duckdb {

class OptionalMatchStackFrame : public MatchStackFrame {
public:
	OptionalMatchStackFrame(match_frame_index_t frame_index, const OptionalMatcher &matcher, MatchState &state)
	    : MatchStackFrame(frame_index, matcher, state), optional_matcher(matcher), child_state(state) {
		if (auto current = child_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
	}

	void Execute(MatchStack &stack) override {
		if (!HasChildResult()) {
			stack.PushChildFrame(*this, optional_matcher.GetChildMatcher(), child_state);
			return;
		}
		auto child_result = TakeChildResult();
		if (!child_result.IsSuccess()) {
			SetResult(match_state.AllocateParseResult<OptionalParseResult>());
			return;
		}
		match_state.token_iterator.SetPosition(child_state.token_iterator);
		if (!child_result.HasParseResult()) {
			SetResult(MatcherResult::Success());
			return;
		}
		auto result = match_state.AllocateParseResult<OptionalParseResult>(child_result.GetParseResult(), start_offset);
		SetResult(result);
	}

private:
	const OptionalMatcher &optional_matcher;
	MatchState child_state;
	optional_idx start_offset;
};

} // namespace duckdb
