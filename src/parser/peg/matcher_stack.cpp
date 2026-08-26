#include "duckdb/parser/peg/matcher_stack.hpp"
#include "duckdb/parser/peg/matcher/choice_matcher.hpp"
#include "duckdb/parser/peg/matcher/list_matcher.hpp"
#include "duckdb/parser/peg/matcher/optional_matcher.hpp"

namespace duckdb {
namespace {

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
		SetResult(match_state.AllocateParseResult<OptionalParseResult>(child_result.GetParseResult(), start_offset));
	}

private:
	const OptionalMatcher &optional_matcher;
	MatchState child_state;
	optional_idx start_offset;
};

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
			D_ASSERT(child_state);
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
		if (child_index >= choice_matcher.matchers.size()) {
			SetResult(MatcherResult::Failure());
			return;
		}
		child_state = make_uniq<MatchState>(match_state);
		stack.PushChildFrame(*this, choice_matcher.matchers[child_index].get(), *child_state);
	}

private:
	const ChoiceMatcher &choice_matcher;
	unique_ptr<MatchState> child_state;
	idx_t child_index = 0;
	optional_idx start_offset;
};

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

} // namespace

MatchStackFrame::MatchStackFrame(match_frame_index_t frame_index_p, const Matcher &matcher_p, MatchState &state_p)
    : frame_index(frame_index_p), matcher(matcher_p), match_state(state_p) {
}

void MatchStackFrame::Execute(MatchStack &) {
	SetResult(matcher.MatchParseResultInternal(match_state));
}

void MatchStackFrame::SetResult(const MatcherResult &result) {
	D_ASSERT(result_state == MatchResultState::NONE);
	result_state = result.IsSuccess() ? MatchResultState::SUCCESS : MatchResultState::FAILURE;
	parse_result = result.GetParseResult();
}

bool MatchStackFrame::HasResult() const {
	return result_state != MatchResultState::NONE;
}

MatcherResult MatchStackFrame::GetResult() const {
	D_ASSERT(HasResult());
	if (result_state != MatchResultState::SUCCESS) {
		return MatcherResult::Failure();
	}
	return MatcherResult::Success(parse_result);
}

void MatchStackFrame::SetChildResult(const MatcherResult &result) {
	D_ASSERT(child_result_state == MatchResultState::NONE);
	child_result_state = result.IsSuccess() ? MatchResultState::SUCCESS : MatchResultState::FAILURE;
	child_parse_result = result.GetParseResult();
}

bool MatchStackFrame::HasChildResult() const {
	return child_result_state != MatchResultState::NONE;
}

MatcherResult MatchStackFrame::TakeChildResult() {
	D_ASSERT(HasChildResult());
	auto result_state = child_result_state;
	auto parse_result = child_parse_result;
	child_result_state = MatchResultState::NONE;
	child_parse_result = nullptr;
	if (result_state != MatchResultState::SUCCESS) {
		return MatcherResult::Failure();
	}
	return MatcherResult::Success(parse_result);
}

match_frame_index_t MatchStack::PushFrame(const Matcher &matcher, MatchState &state) {
	auto frame_index = frames.size();
	switch (matcher.Type()) {
	case MatcherType::OPTIONAL:
		frames.push_back(make_uniq<OptionalMatchStackFrame>(frame_index, matcher.Cast<OptionalMatcher>(), state));
		break;
	case MatcherType::CHOICE:
		frames.push_back(make_uniq<ChoiceMatchStackFrame>(frame_index, matcher.Cast<ChoiceMatcher>(), state));
		break;
	case MatcherType::LIST:
		frames.push_back(make_uniq<ListMatchStackFrame>(frame_index, matcher.Cast<ListMatcher>(), state));
		break;
	default:
		frames.push_back(make_uniq<MatchStackFrame>(frame_index, matcher, state));
		break;
	}
	frame_stack.push_back(frame_index);
	return frame_index;
}

void MatchStack::PushChildFrame(MatchStackFrame &parent, const Matcher &matcher, MatchState &state) {
	D_ASSERT(!frame_stack.empty());
	D_ASSERT(frame_stack.back() == parent.frame_index);
	D_ASSERT(!parent.HasChildResult());
	PushFrame(matcher, state);
}

MatchStackFrame &MatchStack::GetFrame(match_frame_index_t frame_index) {
	D_ASSERT(frame_index < frames.size());
	return *frames[frame_index];
}

void MatchStack::InitializeFrame(MatchStackFrame &frame) {
	auto &matcher = frame.matcher;
	auto &state = frame.match_state;
	if (state.packrat_cache && matcher.IsPackratMemoized() && matcher.GetPackratId().IsValid()) {
		frame.token_index_before = state.token_iterator.Position();
		auto cached_result = state.packrat_cache->Lookup(matcher, frame.token_index_before);
		if (cached_result) {
			state.token_iterator.SetPosition(cached_result->token_index_after);
			state.max_token_index = MaxValue(state.max_token_index, cached_result->max_token_index_seen);
			if (cached_result->success) {
				frame.SetResult(MatcherResult::Success(cached_result->result));
			} else {
				frame.SetResult(MatcherResult::Failure());
			}
			return;
		}
		frame.store_packrat_result = true;
		frame.max_token_index_before = state.GetMaxTokenIndex();
	}
}

void MatchStack::ExecuteFrame(MatchStackFrame &frame) {
	if (frame.state == MatchFrameState::INITIALIZE) {
		InitializeFrame(frame);
		frame.state = MatchFrameState::EXECUTE;
	}
	if (!frame.HasResult()) {
		frame.Execute(*this);
	}
}

MatcherResult MatchStack::FinalizeFrame(MatchStackFrame &frame) {
	auto result = frame.GetResult();
	auto &matcher = frame.matcher;
	auto &state = frame.match_state;
	if (frame.store_packrat_result) {
		ParserPackratEntry cache_entry;
		cache_entry.success = result.IsSuccess();
		cache_entry.token_index_after = state.token_iterator.Position();
		cache_entry.max_token_index_seen = MaxValue(frame.max_token_index_before, state.GetMaxTokenIndex());
		cache_entry.result = result.GetParseResult();
		state.packrat_cache->Store(matcher, frame.token_index_before, std::move(cache_entry));
	}
	auto rule = matcher.GetRule();
	if (result.HasParseResult() && rule) {
		auto parse_result = result.GetParseResult();
		parse_result->SetRule(*rule);
		parse_result->name = rule->name;
	}
	return result;
}

MatcherResult MatchStack::ExecuteInternal(const Matcher &matcher, MatchState &state) {
	D_ASSERT(frames.empty());
	D_ASSERT(frame_stack.empty());
	PushFrame(matcher, state);
	while (!frame_stack.empty()) {
		auto frame_index = frame_stack.back();
		auto &frame = GetFrame(frame_index);
		auto frame_count = frame_stack.size();
		ExecuteFrame(frame);
		if (!frame.HasResult()) {
			D_ASSERT(frame_stack.size() > frame_count);
			continue;
		}
		auto result = FinalizeFrame(frame);
		frame_stack.pop_back();
		D_ASSERT(frame_index + 1 == frames.size());
		frames.pop_back();
		if (frame_stack.empty()) {
			return result;
		}
		auto &parent = GetFrame(frame_stack.back());
		parent.SetChildResult(result);
	}
	throw InternalException("Matcher stack completed without a result");
}

MatcherResult MatchStack::Execute(const Matcher &matcher, MatchState &state) {
	return ExecuteInternal(matcher, state);
}

} // namespace duckdb
