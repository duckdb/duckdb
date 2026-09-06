//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/matcher_stack.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
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
	~MatchStack();

	MatcherResult Execute(const Matcher &matcher, MatchState &state);
	void PushChildFrame(MatchStackFrame &parent, const Matcher &matcher, MatchState &state);

private:
	static bool IsTerminalMatcher(const Matcher &matcher);
	MatcherResult ExecuteTerminalMatcher(const Matcher &matcher, MatchState &state);
	void PushFrame(const Matcher &matcher, MatchState &state);
	template <class FRAME, class... ARGS>
	void AllocateFrame(ARGS &&... args);
	data_ptr_t AllocateFrameMemory(idx_t size);
	void PopFrame();
	void InitializeFrame(MatchStackFrame &frame);
	void ExecuteFrame(MatchStackFrame &frame);
	MatcherResult FinalizeFrame(MatchStackFrame &frame);
	MatcherResult ExecuteInternal(const Matcher &matcher, MatchState &state);

private:
	//! Frames are pushed and popped strictly LIFO, so they are placed in fixed-size blocks that are reused as frames
	//! pop instead of being allocated one by one. An entry records where the block cursor stood before its frame,
	//! which is where the cursor returns when the frame pops.
	struct FrameEntry {
		reference<MatchStackFrame> frame;
		idx_t block_index;
		idx_t block_offset;
	};
	static constexpr idx_t FRAME_BLOCK_SIZE = 16384;

	unsafe_vector<FrameEntry> frames;
	unsafe_vector<AllocatedData> blocks;
	idx_t block_index = 0;
	idx_t block_offset = 0;
};

} // namespace duckdb
