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

using match_frame_index_t = idx_t;

class MatchStack;

//! Locates a MatchState either outside the stack or within a stable frame slot.
struct MatchStateReference {
	explicit MatchStateReference(MatchState &state_p) : external_state(state_p) {
	}
	explicit MatchStateReference(idx_t frame_offset_p) : frame_offset(frame_offset_p) {
	}

	MatchState &Get(MatchStack &stack, match_frame_index_t frame_index);

private:
	optional_ptr<MatchState> external_state;
	idx_t frame_offset = 0;
};

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
	MatchStackFrame(const Matcher &matcher_p, MatchStateReference match_state_p, data_ptr_t process_storage_p,
	                idx_t process_capacity, idx_t process_alignment);

public:
	bool IsInitialized() const;
	MatchState &GetMatchState(MatchStack &stack, match_frame_index_t frame_index);

public:
	const Matcher &matcher;
	MatchStateReference match_state;
	MatchProcessInlineStorage process_inline_storage;
	arena_ptr<MatchProcess> process;
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

	static idx_t FrameHeaderSize();
	static idx_t FrameSlotSize();
	static idx_t FrameSegmentSize();
	void AllocateFrameSegment();
	data_ptr_t GetFrameSegment(idx_t segment_index) const;
	void SetActiveFrameSegment(idx_t segment_index);
	data_ptr_t GetFrameSlot(match_frame_index_t frame_index) const;
	MatchStackFrame &GetFrame(match_frame_index_t frame_index) const;
	MatchStateReference CreateStateReference(MatchState &state) const;
	MatcherResult ExecuteAtomicMatcher(MatchInput input);
	void DestroyTopFrame();
	void PushFrame(MatchInput input);
	void InitializeFrame(MatchStackFrame &frame);
	//! Returns true when the frame has completed.
	bool ExecuteFrame(MatchStackFrame &frame);
	MatcherResult FinalizeFrame(MatchStackFrame &frame);

private:
	friend struct MatchStateReference;
	ArenaAllocator frame_allocator;
	array<data_ptr_t, INLINE_FRAME_SEGMENT_COUNT> inline_frame_segments {};
	vector<data_ptr_t> overflow_frame_segments;
	idx_t frame_segment_count = 0;
	idx_t frame_count = 0;
	data_ptr_t active_frame_segment = nullptr;
	idx_t active_frame_segment_index = DConstants::INVALID_INDEX;
};

} // namespace duckdb
