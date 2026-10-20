#include "duckdb/parser/peg/matcher_stack.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/parser/peg/matcher/optional_matcher.hpp"

namespace duckdb {

MatchStack::MatchStack() {
}

MatchStack::~MatchStack() {
	// Child processes can reference state owned by their parents.
	while (!frames.empty()) {
		DestroyTopFrame();
	}
}

void MatchStack::DestroyTopFrame() {
	D_ASSERT(!frames.empty());
	auto &frame = frames.back();
	auto &process_allocator = frame.match_state.context.process_allocator;
	const auto process_arena_position = frame.process_arena_position;
	frames.pop_back();
	// the processes of a frame and everything they allocated are dead once the frame is popped - release them so the
	// process arena only ever holds the live frames instead of every process the whole statement ever created
	process_allocator.Rewind(process_arena_position);
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

MatchStackFrame::MatchStackFrame(MatchInput input) : matcher(input.matcher), match_state(input.state) {
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
	frames.emplace_back(input);
	frames.back().process_arena_position = input.state.context.process_allocator.GetPosition();
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
	if (!child->matcher.IsStructural() && child->matcher.IsAtomic()) {
		frame.child_result = ExecuteAtomicMatcher(*child);
		return false;
	}
	if (child->state.BuildParseResult()) {
		// skip sub-matchers that certainly cannot match at the current token (see MatcherFirstSet); such an attempt
		// would fail without consuming a token, so skipping it changes neither the result nor the error position
		if (child->matcher.Type() == MatcherType::OPTIONAL && child->matcher.IsStructural() &&
		    !child->matcher.Cast<OptionalMatcher>().GetChildMatcher().first_set.MightMatch(child->state)) {
			// what the optional's own process returns when its element fails
			child->state.rule = child->matcher.GetRule();
			frame.child_result = child->state.AllocateParseResult<OptionalParseResult>();
			return false;
		}
		if (!child->matcher.first_set.MightMatch(child->state)) {
			frame.child_result = MatcherResult::Failure();
			return false;
		}
	}
	PushFrame(*child);
	return false;
}

MatcherResult MatchStack::FinalizeFrame(MatchStackFrame &frame) {
	if (!frame.result) {
		throw InternalException("Trying to finalize a frame without a stored result");
	}
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
	frames.reserve(INITIAL_FRAME_CAPACITY);
	PushFrame(input);
	while (!frames.empty()) {
		if (!ExecuteFrame(frames.back())) {
			continue;
		}
		auto result = FinalizeFrame(frames.back());
		DestroyTopFrame();
		if (frames.empty()) {
			return result;
		}
		auto &parent = frames.back();
		D_ASSERT(!parent.child_result);
		parent.child_result = result;
	}
	throw InternalException("Matcher stack completed without a result");
}

} // namespace duckdb
