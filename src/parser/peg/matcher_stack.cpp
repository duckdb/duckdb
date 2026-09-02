#include "duckdb/parser/peg/matcher_stack.hpp"

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

MatchStackFrame::MatchStackFrame(MatchInput input) : matcher(input.matcher), match_state(input.state) {
}

void MatchStack::PushFrame(MatchInput input) {
	input.state.rule = input.matcher.GetRule();
	frames.push_back(make_uniq<MatchStackFrame>(input));
}

void MatchStack::InitializeFrame(MatchStackFrame &frame) {
	auto &matcher = frame.matcher;
	auto &state = frame.match_state;
	if (PackratMatchState::IsEnabled(matcher, state)) {
		auto cached_result = frame.packrat_state.TryLoadCachedResult(matcher, state);
		if (cached_result) {
			frame.result = *cached_result;
			return;
		}
	}
	frame.continuation = matcher.StartMatch(state);
}

void MatchStack::ExecuteFrame(MatchStackFrame &frame) {
	if (!frame.initialized) {
		InitializeFrame(frame);
		frame.initialized = true;
	}
	if (frame.result) {
		return;
	}
	D_ASSERT(frame.continuation);
	auto step = frame.continuation->Resume(std::move(frame.child_result));
	frame.child_result.reset();
	auto child = step.GetChild();
	if (!child) {
		frame.result = step.GetResult();
		return;
	}
	PushFrame(*child);
}

MatcherResult MatchStack::FinalizeFrame(MatchStackFrame &frame) {
	D_ASSERT(frame.result);
	auto result = *frame.result;
	auto &matcher = frame.matcher;
	auto &state = frame.match_state;
	frame.packrat_state.StoreResult(matcher, state, result);
	return result;
}

MatcherResult MatchStack::Execute(MatchInput input) {
	D_ASSERT(frames.empty());
	PushFrame(input);
	while (!frames.empty()) {
		auto &frame = *frames.back();
		ExecuteFrame(frame);
		if (!frame.result) {
			continue;
		}
		auto result = FinalizeFrame(frame);
		frames.pop_back();
		if (frames.empty()) {
			return result;
		}
		auto &parent = *frames.back();
		D_ASSERT(!parent.child_result);
		parent.child_result = result;
	}
	throw InternalException("Matcher stack completed without a result");
}

} // namespace duckdb
