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

class MatchStack;

enum class MatchFrameState : uint8_t { INITIALIZE, EXECUTE };
enum class MatchResultState : uint8_t { NONE, FAILURE, SUCCESS };

struct MatchStackFrame {
	MatchStackFrame(match_frame_index_t frame_index, const Matcher &matcher, MatchState &state);
	virtual ~MatchStackFrame() = default;

	virtual void Execute(MatchStack &stack);
	void SetResult(const MatcherResult &result);
	bool HasResult() const;
	MatcherResult GetResult() const;
	void SetChildResult(const MatcherResult &result);
	bool HasChildResult() const;
	MatcherResult TakeChildResult();

	const match_frame_index_t frame_index;
	const Matcher &matcher;
	MatchState &match_state;
	MatchFrameState state = MatchFrameState::INITIALIZE;
	MatchResultState result_state = MatchResultState::NONE;
	optional_ptr<ParseResult> parse_result;
	MatchResultState child_result_state = MatchResultState::NONE;
	optional_ptr<ParseResult> child_parse_result;
	bool store_packrat_result = false;
	idx_t token_index_before = 0;
	idx_t max_token_index_before = 0;
};

class MatchStack {
public:
	MatcherResult Execute(const Matcher &matcher, MatchState &state);
	void PushChildFrame(MatchStackFrame &parent, const Matcher &matcher, MatchState &state);

private:
	void PushFrame(const Matcher &matcher, MatchState &state);
	void InitializeFrame(MatchStackFrame &frame);
	void ExecuteFrame(MatchStackFrame &frame);
	MatcherResult FinalizeFrame(MatchStackFrame &frame);
	MatcherResult ExecuteInternal(const Matcher &matcher, MatchState &state);

private:
	vector<unique_ptr<MatchStackFrame>> frames;
};

} // namespace duckdb
