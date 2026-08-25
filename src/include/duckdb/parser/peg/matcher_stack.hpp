//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/matcher_stack.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

using match_frame_index_t = idx_t;

enum class MatchFrameState : uint8_t { INITIALIZE, WAITING };

struct MatchStackFrame {
	MatchStackFrame(match_frame_index_t frame_index, const Matcher &matcher, MatchState &state);

	void SetResult(const MatcherResult &result);
	MatcherResult GetResult() const;

	const match_frame_index_t frame_index;
	const Matcher &matcher;
	MatchState &match_state;
	MatchFrameState state = MatchFrameState::INITIALIZE;
	bool result_ready = false;
	bool success = false;
	optional_ptr<ParseResult> parse_result;
	bool store_packrat_result = false;
	idx_t token_index_before = 0;
	idx_t max_token_index_before = 0;
};

class MatchStack {
public:
	MatcherResult Execute(const Matcher &matcher, MatchState &state);

private:
	match_frame_index_t PushFrame(const Matcher &matcher, MatchState &state);
	MatchStackFrame &GetFrame(match_frame_index_t frame_index);
	void InitializeFrame(MatchStackFrame &frame);
	MatcherResult FinalizeFrame(MatchStackFrame &frame);
	MatcherResult ExecuteInternal(const Matcher &matcher, MatchState &state);

private:
	vector<unique_ptr<MatchStackFrame>> frames;
	vector<match_frame_index_t> frame_stack;
};

} // namespace duckdb
