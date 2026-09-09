#include "duckdb/parser/peg/matcher_stack.hpp"
#include "duckdb/common/optional.hpp"

namespace duckdb {

namespace {

class InlineProcessStorageScope {
public:
	InlineProcessStorageScope(MatchContext &context_p, MatchProcessInlineStorage &storage)
	    : context(context_p), previous(context.process_inline_storage) {
		context.process_inline_storage = storage;
	}

	~InlineProcessStorageScope() {
		context.process_inline_storage = previous;
	}

private:
	MatchContext &context;
	optional_ptr<MatchProcessInlineStorage> previous;
};

} // namespace

MatchState &MatchStateReference::Get(MatchStack &stack) {
	if (!frame_index.IsValid()) {
		D_ASSERT(external_state);
		return *external_state;
	}
	D_ASSERT(!external_state);
	D_ASSERT(frame_offset < MatchStack::FrameSlotSize());
	auto frame_slot = stack.GetFrameSlot(frame_index.GetIndex());
	return *reinterpret_cast<MatchState *>(frame_slot + frame_offset);
}

MatchStack::MatchStack() : frame_allocator(Allocator::DefaultAllocator(), FrameSegmentSize()) {
}

MatchStack::~MatchStack() {
	// Child processes can reference state owned by their parents.
	while (frame_count > 0) {
		DestroyTopFrame();
	}
}

idx_t MatchStack::FrameHeaderSize() {
	auto process_alignment = BuiltinMatchProcessAlignment();
	D_ASSERT(process_alignment <= alignof(idx_t));
	return AlignValue<idx_t>(sizeof(MatchStackFrame), process_alignment);
}

idx_t MatchStack::FrameSlotSize() {
	return AlignValue<idx_t>(FrameHeaderSize() + BuiltinMatchProcessSize());
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
	while (segment_index >= frame_segment_count) {
		AllocateFrameSegment();
	}
	active_frame_segment = GetFrameSegment(segment_index);
	active_frame_segment_index = segment_index;
}

data_ptr_t MatchStack::GetFrameSlot(match_frame_index_t frame_index) const {
	D_ASSERT(frame_index < frame_segment_count * FRAME_SEGMENT_CAPACITY);
	auto segment_index = frame_index / FRAME_SEGMENT_CAPACITY;
	auto slot_index = frame_index % FRAME_SEGMENT_CAPACITY;
	return GetFrameSegment(segment_index) + slot_index * FrameSlotSize();
}

MatchStackFrame &MatchStack::GetFrame(match_frame_index_t frame_index) const {
	D_ASSERT(frame_index < frame_count);
	return *reinterpret_cast<MatchStackFrame *>(GetFrameSlot(frame_index));
}

MatchStateReference MatchStack::CreateStateReference(MatchState &state, optional_idx parent_frame) const {
	if (parent_frame.IsValid()) {
		auto frame_slot = GetFrameSlot(parent_frame.GetIndex());
		auto state_address = reinterpret_cast<uintptr_t>(&state);
		auto slot_address = reinterpret_cast<uintptr_t>(frame_slot);
		if (state_address >= slot_address && state_address < slot_address + FrameSlotSize()) {
			return MatchStateReference(parent_frame.GetIndex(), state_address - slot_address);
		}
	}
	return MatchStateReference(state);
}

void MatchStack::DestroyTopFrame() {
	D_ASSERT(frame_count > 0);
	auto &frame = GetFrame(frame_count - 1);
	frame.~MatchStackFrame();
	frame_count--;
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

MatchStackFrame::MatchStackFrame(const Matcher &matcher_p, MatchStateReference match_state_p,
                                 data_ptr_t process_storage_p, idx_t process_capacity, idx_t process_alignment)
    : matcher(matcher_p), match_state(std::move(match_state_p)),
      process_inline_storage(process_storage_p, process_capacity, process_alignment) {
}

bool MatchStackFrame::IsInitialized() const {
	return process || result;
}

MatchState &MatchStackFrame::GetMatchState(MatchStack &stack) {
	return match_state.Get(stack);
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

void MatchStack::PushFrame(MatchInput input, optional_idx parent_frame) {
	input.state.rule = input.matcher.GetRule();
	auto frame_index = frame_count;
	auto segment_index = frame_index / FRAME_SEGMENT_CAPACITY;
	if (segment_index != active_frame_segment_index) {
		SetActiveFrameSegment(segment_index);
	}
	auto frame_slot = GetFrameSlot(frame_index);
	auto process_storage = frame_slot + FrameHeaderSize();
	auto state_reference = CreateStateReference(input.state, parent_frame);
	new (frame_slot) MatchStackFrame(input.matcher, std::move(state_reference), process_storage,
	                                 BuiltinMatchProcessSize(), BuiltinMatchProcessAlignment());
	frame_count++;
}

void MatchStack::InitializeFrame(MatchStackFrame &frame) {
	auto &matcher = frame.matcher;
	auto &state = frame.GetMatchState(*this);
	if (PackratMatchState::IsEnabled(matcher, state)) {
		auto cached_result = frame.packrat_state.TryLoadCachedResult(matcher, state);
		if (cached_result) {
			frame.result = *cached_result;
			return;
		}
	}
	InlineProcessStorageScope storage_scope(state.context, frame.process_inline_storage);
	frame.process = matcher.StartMatch(state);
}

bool MatchStack::ExecuteFrame(MatchStackFrame &frame) {
	if (!frame.IsInitialized()) {
		InitializeFrame(frame);
		D_ASSERT(frame.IsInitialized());
	}
	if (frame.result) {
		return true;
	}
	D_ASSERT(frame.process);
	auto step = frame.process->Resume(frame.child_result);
	frame.child_result.reset();
	auto child = step.GetChild();
	if (!child) {
		frame.result = step.GetResult();
		return true;
	}
	if (child->matcher.IsAtomic()) {
		frame.child_result = ExecuteAtomicMatcher(*child);
		return false;
	}
	PushFrame(*child, optional_idx(frame_count - 1));
	return false;
}

MatcherResult MatchStack::FinalizeFrame(MatchStackFrame &frame) {
	if (!frame.result) {
		throw InternalException("Trying to finalize a frame without a stored result");
	}
	auto result = *frame.result;
	auto &matcher = frame.matcher;
	auto &state = frame.GetMatchState(*this);
	frame.packrat_state.StoreResult(matcher, state, result);
	return result;
}

MatcherResult MatchStack::Execute(MatchInput input) {
	D_ASSERT(frame_count == 0);
	if (input.matcher.IsAtomic()) {
		return ExecuteAtomicMatcher(input);
	}
	PushFrame(input);
	while (frame_count > 0) {
		if (!ExecuteFrame(GetFrame(frame_count - 1))) {
			continue;
		}
		auto result = FinalizeFrame(GetFrame(frame_count - 1));
		DestroyTopFrame();
		if (frame_count == 0) {
			return result;
		}
		auto &parent = GetFrame(frame_count - 1);
		D_ASSERT(!parent.child_result);
		parent.child_result = result;
	}
	throw InternalException("Matcher stack completed without a result");
}

} // namespace duckdb
