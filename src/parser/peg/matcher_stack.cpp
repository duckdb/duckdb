#include "duckdb/parser/peg/matcher_stack.hpp"

namespace duckdb {

MatchStackFrame::MatchStackFrame(match_frame_index_t frame_index_p, const Matcher &matcher_p, MatchState &state_p)
    : frame_index(frame_index_p), matcher(matcher_p), match_state(state_p) {
}

void MatchStackFrame::SetResult(const MatcherResult &result) {
	result_ready = true;
	success = result.IsSuccess();
	parse_result = result.GetParseResult();
}

MatcherResult MatchStackFrame::GetResult() const {
	D_ASSERT(result_ready);
	if (!result_ready || !success) {
		return MatcherResult::Failure();
	}
	return MatcherResult::Success(parse_result);
}

match_frame_index_t MatchStack::PushFrame(const Matcher &matcher, MatchState &state) {
	auto frame_index = frames.size();
	frames.push_back(make_uniq<MatchStackFrame>(frame_index, matcher, state));
	frame_stack.push_back(frame_index);
	return frame_index;
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
	frame.SetResult(matcher.MatchParseResultInternal(state));
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
		switch (frame.state) {
		case MatchFrameState::INITIALIZE:
			frame.state = MatchFrameState::WAITING;
			InitializeFrame(frame);
			break;
		case MatchFrameState::WAITING: {
			auto result = FinalizeFrame(frame);
			frame_stack.pop_back();
			D_ASSERT(frame_stack.empty());
			return result;
		}
		default:
			throw InternalException("Invalid matcher frame state");
		}
	}
	throw InternalException("Matcher stack completed without a result");
}

MatcherResult MatchStack::Execute(const Matcher &matcher, MatchState &state) {
	return ExecuteInternal(matcher, state);
}

} // namespace duckdb
