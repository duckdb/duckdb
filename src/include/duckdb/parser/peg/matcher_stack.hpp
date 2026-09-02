//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/matcher_stack.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

using match_frame_index_t = idx_t;

class MatchStack;

enum class MatchFrameState : uint8_t { INITIALIZE, EXECUTE };
enum class MatchResultState : uint8_t { NONE, FAILURE, SUCCESS };

struct PackratMatchState {
	static bool IsEnabled(const Matcher &matcher, const MatchState &state) {
		return state.packrat_cache && matcher.IsPackratMemoized() && matcher.GetPackratId().IsValid();
	}

	optional<MatcherResult> TryLoadCachedResult(const Matcher &matcher, MatchState &state);
	void StoreResult(const Matcher &matcher, MatchState &state, const MatcherResult &result) const;

private:
	optional_idx token_index_before;
	idx_t max_token_index_before = 0;
};

struct MatchStackFrame {
	MatchStackFrame(match_frame_index_t frame_index, const Matcher &matcher, MatchState &state);
	virtual ~MatchStackFrame() = default;

	virtual void Execute(MatchStack &stack) = 0;
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
	PackratMatchState packrat_state;
};

class MatchStack {
public:
	MatcherResult Execute(const Matcher &matcher, MatchState &state);
	void PushChildFrame(MatchStackFrame &parent, const Matcher &matcher, MatchState &state);

private:
	static bool IsTerminalMatcher(const Matcher &matcher);
	MatcherResult ExecuteTerminalMatcher(const Matcher &matcher, MatchState &state);
	void PushFrame(const Matcher &matcher, MatchState &state);
	void InitializeFrame(MatchStackFrame &frame);
	void ExecuteFrame(MatchStackFrame &frame);
	MatcherResult FinalizeFrame(MatchStackFrame &frame);
	MatcherResult ExecuteInternal(const Matcher &matcher, MatchState &state);

private:
	vector<unique_ptr<MatchStackFrame>> frames;
};

} // namespace duckdb
