#include "duckdb/parser/peg/matcher_stack.hpp"

namespace duckdb {

MatchStack::MatchStack()
    : frame_allocator(Allocator::DefaultAllocator(), FrameSegmentSize() / 2), process_allocator(frame_allocator) {
	frames.reserve(FRAME_SEGMENT_CAPACITY);
}

MatchStack::~MatchStack() {
	while (!frames.empty()) {
		DestroyTopFrame();
	}
}

idx_t MatchStack::FrameSlotSize() {
	static_assert(alignof(MatchStackFrame) <= alignof(idx_t), "Matcher frame alignment is too large");
	return AlignValue<idx_t>(sizeof(MatchStackFrame));
}

idx_t MatchStack::FrameSegmentSize() {
	return FrameSlotSize() * FRAME_SEGMENT_CAPACITY;
}

void MatchStack::AllocateFrameSegment() {
	auto frame_segment = frame_allocator.AllocateAligned(FrameSegmentSize());
	if (frame_segment_count < INLINE_FRAME_SEGMENT_COUNT) {
		inline_frame_segments[frame_segment_count] = frame_segment;
	} else {
		overflow_frame_segments.push_back(frame_segment);
	}
	frame_segment_count++;
}

data_ptr_t MatchStack::GetFrameSegment(idx_t segment_index) const {
	D_ASSERT(segment_index < frame_segment_count);
	if (segment_index < INLINE_FRAME_SEGMENT_COUNT) {
		return inline_frame_segments[segment_index];
	}
	return overflow_frame_segments[segment_index - INLINE_FRAME_SEGMENT_COUNT];
}

void MatchStack::SetActiveFrameSegment(idx_t segment_index) {
	if (segment_index >= frame_segment_count) {
		frames.reserve((segment_index + 1) * FRAME_SEGMENT_CAPACITY);
		do {
			AllocateFrameSegment();
		} while (segment_index >= frame_segment_count);
	}
	active_frame_segment = GetFrameSegment(segment_index);
	active_frame_segment_index = segment_index;
}

data_ptr_t MatchStack::AllocateFrameSlot() {
	auto frame_index = frames.size();
	auto segment_index = frame_index / FRAME_SEGMENT_CAPACITY;
	if (segment_index != active_frame_segment_index) {
		SetActiveFrameSegment(segment_index);
	}
	auto slot_index = frame_index % FRAME_SEGMENT_CAPACITY;
	return active_frame_segment + slot_index * FrameSlotSize();
}

void MatchStack::DestroyTopFrame() {
	D_ASSERT(!frames.empty());
	auto &frame = frames.back().get();
	auto process_position = frame.process_position;
	frames.pop_back();
	frame.~MatchStackFrame();
	process_allocator.Rewind(process_position);
}

optional<MatcherResult> PackratMatchState::TryLoadCachedResult(const Matcher &matcher, MatchState &state) {
	D_ASSERT(IsEnabled(matcher, state));
	auto token_index = state.token_iterator.Position();
	auto cached_result = state.context.packrat_cache->Lookup(matcher, token_index);
	if (!cached_result) {
		token_index_before = token_index;
		max_token_index_before = state.GetMaxTokenIndex();
		return nullopt;
	}

	state.token_iterator.SetPosition(cached_result->token_index_after);
	state.context.max_token_index = MaxValue(state.context.max_token_index, cached_result->max_token_index_seen);
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
	state.context.packrat_cache->Store(matcher, token_index_before.GetIndex(), cache_entry);
}

MatchStackFrame::MatchStackFrame(MatchInput input, MatchProcessAllocator::Position process_position_p)
    : matcher(input.matcher), match_state(input.state), process_position(process_position_p) {
}

bool MatchStackFrame::IsInitialized() const {
	return process || result;
}

MatcherResult MatchStack::ExecuteAtomicMatcher(MatchInput input) {
	auto &matcher = input.matcher;
	auto &state = input.state;
	D_ASSERT(matcher.IsAtomic());
	state.rule = matcher.GetRule();

	PackratMatchState packrat_state;
	if (PackratMatchState::IsEnabled(matcher, state)) {
		auto cached_result = packrat_state.TryLoadCachedResult(matcher, state);
		if (cached_result) {
			return *cached_result;
		}
	}

	auto result = static_cast<const AtomicMatcher &>(matcher).MatchAtomic(state);
	packrat_state.StoreResult(matcher, state, result);
	return result;
}

void MatchStack::PushFrame(MatchInput input) {
	input.state.rule = input.matcher.GetRule();
	auto frame_slot = AllocateFrameSlot();
	frames.push_back(*new (frame_slot) MatchStackFrame(input, process_allocator.GetPosition()));
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
	frame.process = matcher.StartMatch(state, process_allocator);
}

void MatchStack::ExecuteFrame(MatchStackFrame &frame) {
	if (!frame.IsInitialized()) {
		InitializeFrame(frame);
		D_ASSERT(frame.IsInitialized());
	}
	if (frame.result) {
		return;
	}
	D_ASSERT(frame.process);
	auto step = frame.process->Resume(frame.child_result);
	frame.child_result.reset();
	auto child = step.GetChild();
	if (!child) {
		frame.result = step.GetResult();
		return;
	}
	if (child->matcher.IsAtomic()) {
		frame.child_result = ExecuteAtomicMatcher(*child);
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
	if (input.matcher.IsAtomic()) {
		return ExecuteAtomicMatcher(input);
	}
	PushFrame(input);
	while (!frames.empty()) {
		auto &frame = frames.back().get();
		ExecuteFrame(frame);
		if (!frame.result) {
			continue;
		}
		auto result = FinalizeFrame(frame);
		DestroyTopFrame();
		if (frames.empty()) {
			return result;
		}
		auto &parent = frames.back().get();
		D_ASSERT(!parent.child_result);
		parent.child_result = result;
	}
	throw InternalException("Matcher stack completed without a result");
}

} // namespace duckdb
