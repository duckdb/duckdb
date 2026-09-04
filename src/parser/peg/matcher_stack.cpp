#include "duckdb/parser/peg/matcher_stack.hpp"
#include "duckdb/parser/peg/matcher_stack/choice_match_stack_frame.hpp"
#include "duckdb/parser/peg/matcher_stack/list_match_stack_frame.hpp"
#include "duckdb/parser/peg/matcher_stack/optional_match_stack_frame.hpp"
#include "duckdb/parser/peg/matcher_stack/repeat_match_stack_frame.hpp"

namespace duckdb {

optional<MatcherResult> PackratMatchState::TryLoadCachedResult(const Matcher &matcher, MatchState &state) {
	D_ASSERT(IsEnabled(matcher, state));
	auto token_index = state.token_iterator.Position();
	auto cached_result = state.packrat_cache->Lookup(matcher, token_index);
	if (!cached_result) {
		token_index_before = token_index;
		max_token_index_before = state.GetMaxTokenIndex();
		return nullopt;
	}

	state.token_iterator.SetPosition(cached_result->token_index_after);
	state.max_token_index = MaxValue(state.max_token_index, cached_result->max_token_index_seen);
	if (cached_result->success) {
		return MatcherResult::Success(cached_result->result);
	}
	return MatcherResult::Failure();
}

void PackratMatchState::StoreResult(const Matcher &matcher, MatchState &state, const MatcherResult &result) const {
	if (!token_index_before.IsValid()) {
		return;
	}
	ParserPackratEntry cache_entry;
	cache_entry.success = result.IsSuccess();
	cache_entry.token_index_after = state.token_iterator.Position();
	cache_entry.max_token_index_seen = MaxValue(max_token_index_before, state.GetMaxTokenIndex());
	cache_entry.result = result.GetParseResult();
	state.packrat_cache->Store(matcher, token_index_before.GetIndex(), cache_entry);
}

MatchStackFrame::MatchStackFrame(match_frame_index_t frame_index_p, const Matcher &matcher_p, MatchState &state_p)
    : frame_index(frame_index_p), matcher(matcher_p), match_state(state_p) {
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

bool MatchStack::IsTerminalMatcher(const Matcher &matcher) {
	switch (matcher.Type()) {
	case MatcherType::KEYWORD:
	case MatcherType::VARIABLE:
	case MatcherType::STRING_LITERAL:
	case MatcherType::NUMBER_LITERAL:
	case MatcherType::OPERATOR:
	case MatcherType::END_OF_INPUT:
		return true;
	case MatcherType::OPTIONAL:
	case MatcherType::CHOICE:
	case MatcherType::LIST:
	case MatcherType::REPEAT:
		return false;
	default:
		throw InternalException("Unsupported matcher type in heap-based parser");
	}
}

MatcherResult MatchStack::ExecuteTerminalMatcher(const Matcher &matcher, MatchState &state) {
	D_ASSERT(IsTerminalMatcher(matcher));
	state.rule = matcher.GetRule();
	if (!PackratMatchState::IsEnabled(matcher, state)) {
		return matcher.MatchParseResultInternal(state);
	}

	PackratMatchState packrat_state;
	auto cached_result = packrat_state.TryLoadCachedResult(matcher, state);
	if (cached_result) {
		return *cached_result;
	}

	auto result = matcher.MatchParseResultInternal(state);
	packrat_state.StoreResult(matcher, state, result);
	return result;
}

void MatchStack::PushFrame(const Matcher &matcher, MatchState &state) {
	state.rule = matcher.GetRule();
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
	case MatcherType::REPEAT:
		frames.push_back(make_uniq<RepeatMatchStackFrame>(frame_index, matcher.Cast<RepeatMatcher>(), state));
		break;
	case MatcherType::KEYWORD:
	case MatcherType::VARIABLE:
	case MatcherType::STRING_LITERAL:
	case MatcherType::NUMBER_LITERAL:
	case MatcherType::OPERATOR:
	case MatcherType::END_OF_INPUT:
		throw InternalException("Terminal matcher cannot create a heap-based parser frame");
	default:
		throw InternalException("Unsupported matcher type in heap-based parser");
	}
}

void MatchStack::PushChildFrame(MatchStackFrame &parent, const Matcher &matcher, MatchState &state) {
	D_ASSERT(!frames.empty());
	D_ASSERT(frames.back()->frame_index == parent.frame_index);
	D_ASSERT(!parent.HasChildResult());
	if (IsTerminalMatcher(matcher)) {
		parent.SetChildResult(ExecuteTerminalMatcher(matcher, state));
		return;
	}
	PushFrame(matcher, state);
}

void MatchStack::InitializeFrame(MatchStackFrame &frame) {
	auto &matcher = frame.matcher;
	auto &state = frame.match_state;
	if (!PackratMatchState::IsEnabled(matcher, state)) {
		return;
	}
	auto cached_result = frame.packrat_state.TryLoadCachedResult(matcher, state);
	if (cached_result) {
		frame.SetResult(*cached_result);
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
	frame.packrat_state.StoreResult(matcher, state, result);
	return result;
}

MatcherResult MatchStack::ExecuteInternal(const Matcher &matcher, MatchState &state) {
	D_ASSERT(frames.empty());
	if (IsTerminalMatcher(matcher)) {
		return ExecuteTerminalMatcher(matcher, state);
	}
	PushFrame(matcher, state);
	while (!frames.empty()) {
		auto &frame = *frames.back();
		auto frame_count = frames.size();
		ExecuteFrame(frame);
		if (!frame.HasResult()) {
			D_ASSERT(frames.size() > frame_count || frame.HasChildResult());
			continue;
		}
		auto result = FinalizeFrame(frame);
		frames.pop_back();
		if (frames.empty()) {
			return result;
		}
		auto &parent = *frames.back();
		parent.SetChildResult(result);
	}
	throw InternalException("Matcher stack completed without a result");
}

MatcherResult MatchStack::Execute(const Matcher &matcher, MatchState &state) {
	return ExecuteInternal(matcher, state);
}

} // namespace duckdb
