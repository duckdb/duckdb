//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/matcher_stack.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

struct PackratMatchState {
	static bool IsEnabled(const Matcher &matcher, const MatchState &state) {
		return state.context.packrat_cache && matcher.IsPackratMemoized() && matcher.GetPackratId().IsValid();
	}

	optional<MatcherResult> TryLoadCachedResult(const Matcher &matcher, MatchState &state);
	void StoreResult(const Matcher &matcher, MatchState &state, const MatcherResult &result) const;

private:
	optional_idx token_index_before;
	idx_t max_token_index_before = 0;
};

struct MatchStackFrame {
public:
	explicit MatchStackFrame(MatchInput input);

public:
	bool IsInitialized() const;

public:
	const Matcher &matcher;
	MatchState &match_state;
	unique_ptr<MatchProcess> process;
	optional<MatcherResult> child_result;
	optional<MatcherResult> result;
	PackratMatchState packrat_state;
};

class MatchStack {
public:
	MatchStack();
	~MatchStack();

	MatcherResult Execute(MatchInput input);

private:
	static constexpr idx_t FRAME_SEGMENT_CAPACITY = 64;
	static constexpr idx_t INLINE_FRAME_SEGMENT_COUNT = 2;

	static idx_t FrameSlotSize();
	static idx_t FrameSegmentSize();
	MatcherResult ExecuteAtomicMatcher(MatchInput input);
	void AllocateFrameSegment();
	data_ptr_t GetFrameSegment(idx_t segment_index) const;
	void SetActiveFrameSegment(idx_t segment_index);
	data_ptr_t AllocateFrameSlot();
	void DestroyTopFrame();
	void PushFrame(MatchInput input);
	void InitializeFrame(MatchStackFrame &frame);
	void ExecuteFrame(MatchStackFrame &frame);
	MatcherResult FinalizeFrame(MatchStackFrame &frame);

private:
	ArenaAllocator frame_allocator;
	array<data_ptr_t, INLINE_FRAME_SEGMENT_COUNT> inline_frame_segments {};
	vector<data_ptr_t> overflow_frame_segments;
	idx_t frame_segment_count = 0;
	data_ptr_t active_frame_segment = nullptr;
	idx_t active_frame_segment_index = DConstants::INVALID_INDEX;
	vector<reference<MatchStackFrame>> frames;
};

} // namespace duckdb
