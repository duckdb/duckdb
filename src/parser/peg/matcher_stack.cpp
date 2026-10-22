#include "duckdb/parser/peg/matcher_stack.hpp"
#include "duckdb/parser/peg/matcher/list_matcher.hpp"
#include "duckdb/parser/peg/matcher/choice_matcher.hpp"
#include "duckdb/parser/peg/matcher/optional_matcher.hpp"
#include "duckdb/parser/peg/matcher/repeat_matcher.hpp"

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

struct MatchStackFrame : public MatchProcess {
	MatchStackFrame(MatcherType kind_p, MatchInput input, ArenaAllocator::Position mark_p,
	                const PackratMatchState &packrat_p)
	    : kind(kind_p), input_matcher(input.matcher), state(input.state), mark(mark_p), packrat(packrat_p) {
	}
	MatchStep Resume(optional<MatcherResult> child_result) final;
	MatcherType kind;
	const Matcher &input_matcher;
	MatchState &state;
	ArenaAllocator::Position mark;
	PackratMatchState packrat;
};
namespace {

class ListMatchFrame final : public MatchStackFrame {
public:
	struct PendingList {
		PendingList(const ListMatcher &matcher, optional_ptr<PendingList> parent) : matcher(matcher), parent(parent) {
		}

		const ListMatcher &matcher;
		optional_ptr<PendingList> parent;
	};

public:
	ListMatchFrame(const ListMatcher &matcher_p, MatchState &state_p, ArenaAllocator::Position mark = {},
	               const PackratMatchState &packrat = {})
	    : MatchStackFrame(MatcherType::LIST, {matcher_p, state_p}, mark, packrat), matcher(matcher_p),
	      list_state(state_p), results(state_p.context.process_allocator) {
		saved_suggestion_size = matcher_p.suppress_suggestions ? list_state.context.suggestions.size() : 0;
		auto current = list_state.token_iterator.Current();
		if (current) {
			start_offset = optional_idx(current->offset);
		}
		if (state.BuildParseResult() && (!current || current->type != TokenType::END_OF_INPUT_AUTOCOMPLETE)) {
			while (auto child = matcher.get().GetChainChild()) {
				if (!child->first_set.MightMatch(list_state)) {
					break;
				}
				pending = state.context.process_allocator.Make<PendingList>(matcher.get(), pending);
				matcher = *child;
			}
		}
		ReserveResults();
	}

public:
	bool CanCollapseEmptyTail() {
		if (!state.BuildParseResult() || !FindCollapsibleResult()) {
			return false;
		}
		auto current = list_state.token_iterator.Current();
		if (current && current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
			return false;
		}
		for (idx_t i = child_index; i < matcher.get().matchers.size(); i++) {
			auto &child = matcher.get().matchers[i].get();
			if (child.Type() != MatcherType::OPTIONAL || !child.IsStructural() ||
			    child.Cast<OptionalMatcher>().GetChildMatcher().first_set.MightMatch(list_state)) {
				return false;
			}
		}
		return true;
	}

	void ReserveResults() {
		if (state.BuildParseResult()) {
			results.reserve(matcher.get().matchers.size());
		}
	}

	MatcherResult CompleteList() {
		auto &current_matcher = matcher.get();
		DiscardSuggestions();
		if (current_matcher.IsCollapsible()) {
			auto collapsible = FindCollapsibleResult();
			if (collapsible) {
				collapsible->collapsed = true;
				return MatcherResult::Success(collapsible);
			}
		}
		auto &result_state = pending ? list_state : state;
		if (pending) {
			result_state.rule = current_matcher.GetRule();
		}
		auto named_matcher = current_matcher.HasName() ? &current_matcher : nullptr;
		return result_state.AllocateParseResult<ListParseResult>(state.context.allocator.MakeChildren(results),
		                                                         named_matcher, start_offset);
	}

	//! The child that can stand in for this rule's own result, or nullptr when the rule has to build one
	optional_ptr<ParseResult> FindCollapsibleResult() const {
		optional_ptr<ParseResult> collapsible;
		for (auto &child : results) {
			auto &child_result = child.get();
			// an optional that matched nothing carries no value, so it does not stop the rule from collapsing
			if (child_result.type == ParseResultType::OPTIONAL &&
			    !child_result.Cast<OptionalParseResult>().HasResult()) {
				continue;
			}

			// a second child with a result means the rule combines them rather than forwarding one of them
			if (collapsible) {
				return nullptr;
			}

			collapsible = child_result;
		}

		// only results that carry a rule of their own are collapsible into this one, since the result is
		// transformed by that rule
		if (collapsible && !collapsible->GetRule()) {
			return nullptr;
		}

		// null when no child produced a result, so a rule that matched empty still gets a result of its own
		return collapsible;
	}

	void DiscardSuggestions() {
		if (!matcher.get().suppress_suggestions) {
			return;
		}
		list_state.context.suggestions.erase(list_state.context.suggestions.begin() +
		                                         NumericCast<int64_t>(saved_suggestion_size),
		                                     list_state.context.suggestions.end());
	}

public:
	reference<const ListMatcher> matcher;
	MatchState list_state;
	arena_vector<reference<ParseResult>> results;
	//! First-child ancestors share this cursor until their tails are reached.
	optional_ptr<PendingList> pending;
	idx_t child_index = 0;
	idx_t saved_suggestion_size = 0;
	optional_idx start_offset;
	bool awaiting_child = false;
};
class ChoiceMatchFrame final : public MatchStackFrame {
public:
	ChoiceMatchFrame(const ChoiceMatcher &matcher_p, MatchState &state_p, ArenaAllocator::Position mark = {},
	                 const PackratMatchState &packrat = {})
	    : MatchStackFrame(MatcherType::CHOICE, {matcher_p, state_p}, mark, packrat), matcher(matcher_p) {
		auto selected = matcher.SelectChild(state);
		single_child = selected.IsValid();
		child_index = single_child ? selected.GetIndex() : 0;

		if (auto current = state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
	}

public:
	const ChoiceMatcher &matcher;
	optional<MatchState> child_state;
	idx_t child_index = 0;
	bool single_child = false;
	optional_idx start_offset;
	bool awaiting_child = false;
};
class OptionalMatchFrame final : public MatchStackFrame {
public:
	OptionalMatchFrame(const OptionalMatcher &matcher_p, MatchState &state_p, ArenaAllocator::Position mark = {},
	                   const PackratMatchState &packrat = {})
	    : MatchStackFrame(MatcherType::OPTIONAL, {matcher_p, state_p}, mark, packrat), matcher(matcher_p),
	      child_state(state_p) {
		if (auto current = child_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
	}

public:
	const OptionalMatcher &matcher;
	MatchState child_state;
	optional_idx start_offset;
	bool awaiting_child = false;
};
class RepeatMatchFrame final : public MatchStackFrame {
public:
	RepeatMatchFrame(const RepeatMatcher &matcher_p, MatchState &state_p, ArenaAllocator::Position mark = {},
	                 const PackratMatchState &packrat = {})
	    : MatchStackFrame(MatcherType::REPEAT, {matcher_p, state_p}, mark, packrat), matcher(matcher_p),
	      repeat_state(state_p), results(state_p.context.process_allocator) {
		if (auto current = repeat_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
	}

public:
	MatcherResult CreateResult() {
		return state.AllocateParseResult<RepeatParseResult>(state.context.allocator.MakeChildren(results),
		                                                    start_offset);
	}

public:
	const RepeatMatcher &matcher;
	MatchState repeat_state;
	arena_vector<reference<ParseResult>> results;
	bool matched_once = false;
	optional_idx start_offset;
	bool awaiting_child = false;
};
struct OpaqueMatchFrame final : MatchStackFrame {
	OpaqueMatchFrame(MatchInput input, ArenaAllocator::Position mark, const PackratMatchState &packrat)
	    : MatchStackFrame(MatcherType::CUSTOM, input, mark, packrat), process(input.matcher.StartMatch(input.state)) {
		D_ASSERT(process);
	}
	arena_ptr<MatchProcess> process;
};

} // namespace

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

optional<MatcherResult> MatchStack::PushFrame(MatchInput input) {
	input.state.rule = input.matcher.GetRule();
	PackratMatchState packrat;
	if (PackratMatchState::IsEnabled(input.matcher, input.state)) {
		auto cached = packrat.TryLoadCachedResult(input.matcher, input.state);
		if (cached) {
			return cached;
		}
	}
	auto &arena = input.state.context.process_allocator;
	auto mark = arena.GetPosition();
	arena_ptr<MatchStackFrame> owned;
	try {
		auto kind = input.matcher.IsStructural() ? input.matcher.Type() : MatcherType::CUSTOM;
		switch (kind) {
		case MatcherType::LIST:
			owned = arena_ptr<MatchStackFrame>(
			    arena.Make<ListMatchFrame>(input.matcher.Cast<ListMatcher>(), input.state, mark, packrat));
			break;

		case MatcherType::CHOICE:
			owned = arena_ptr<MatchStackFrame>(
			    arena.Make<ChoiceMatchFrame>(input.matcher.Cast<ChoiceMatcher>(), input.state, mark, packrat));
			break;

		case MatcherType::OPTIONAL:
			owned = arena_ptr<MatchStackFrame>(
			    arena.Make<OptionalMatchFrame>(input.matcher.Cast<OptionalMatcher>(), input.state, mark, packrat));
			break;

		case MatcherType::REPEAT:
			owned = arena_ptr<MatchStackFrame>(
			    arena.Make<RepeatMatchFrame>(input.matcher.Cast<RepeatMatcher>(), input.state, mark, packrat));
			break;
		default:
			owned = arena_ptr<MatchStackFrame>(arena.Make<OpaqueMatchFrame>(input, mark, packrat));
			break;
		}
		frames.push_back(std::move(owned));
	} catch (...) {
		owned.reset();
		arena.Rewind(mark);
		throw;
	}
	return nullopt;
}

void MatchStack::DestroyTopFrame() {
	D_ASSERT(!frames.empty());
	auto &frame = *frames.back();
	auto &arena = frame.state.context.process_allocator;
	auto mark = frame.mark;
	frames.pop_back();
	arena.Rewind(mark);
}

// Full execution and the public single-step process interface share every state transition.
// Full execution and the public single-step process interface share every state transition.
// Each frame kind sets `has_child` and either `next` (a child to match) or `completed` (its result).
template <bool SINGLE_STEP>
MatchStep MatchStack::ExecuteInternal(optional_ptr<MatchStack> stack, MatchStackFrame &initial_frame,
                                      optional<MatcherResult> child_result) {
	reference<MatchStackFrame> current_frame(initial_frame);
	while (true) {
		auto &frame = current_frame.get();
		optional<MatchInput> next;
		auto completed = MatcherResult::Failure();
		bool has_child = false;
		switch (frame.kind) {
		case MatcherType::LIST: {
			auto &f = static_cast<ListMatchFrame &>(frame);
			D_ASSERT(f.awaiting_child == child_result.has_value());
			// Loops while completed inner lists feed the pending ancestors of a list chain.
			bool list_done = false;
			while (!list_done) {
				if (child_result) {
					f.awaiting_child = false;
					if (!child_result->IsSuccess()) {
						f.DiscardSuggestions();
						break;
					}
					if (child_result->HasParseResult()) {
						f.results.emplace_back(*child_result->GetParseResult());
					}
					f.child_index++;
				}
				auto &current_matcher = f.matcher.get();
				if (f.child_index == 1 && f.child_index < current_matcher.matchers.size() &&
				    current_matcher.IsCollapsible() && f.CanCollapseEmptyTail()) {
					f.child_index = current_matcher.matchers.size();
				}
				while (f.child_index < current_matcher.matchers.size()) {
					auto &child_matcher = current_matcher.matchers[f.child_index].get();
					auto current = f.list_state.token_iterator.Current();
					bool at_autocomplete_cursor = current && current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE;
					if (!at_autocomplete_cursor) {
						f.awaiting_child = true;
						next.emplace(MatchInput {child_matcher, f.list_state});
						has_child = true;
						break;
					}
					if (current_matcher.suppress_suggestions) {
						f.DiscardSuggestions();
						list_done = true;
						break;
					}
					if (child_matcher.AddSuggestion(f.list_state) == SuggestionType::OPTIONAL) {
						f.child_index++;
						continue;
					}
					if (!f.pending) {
						f.state.token_iterator.SetPosition(f.list_state.token_iterator);
					}
					list_done = true;
					break;
				}
				if (has_child || list_done) {
					break;
				}
				if (!f.pending) {
					f.state.token_iterator.SetPosition(f.list_state.token_iterator);
				}
				auto result = f.CompleteList();
				if (!f.pending) {
					completed = result;
					break;
				}
				f.list_state.rule = current_matcher.GetRule();
				f.list_state.added_suggestions.reset();
				f.matcher = f.pending->matcher;
				f.pending = f.pending->parent;
				f.results.clear();
				f.ReserveResults();
				f.child_index = 0;
				child_result = result;
			}
			break;
		}
		case MatcherType::CHOICE: {
			auto &f = static_cast<ChoiceMatchFrame &>(frame);
			D_ASSERT(f.awaiting_child == child_result.has_value());
			if (child_result) {
				f.awaiting_child = false;
				if (!f.child_state) {
					throw InternalException("Choice matcher child state is missing");
				}
				if (child_result->IsSuccess()) {
					f.state.token_iterator.SetPosition(f.child_state->token_iterator);
					if (child_result->HasParseResult()) {
						completed = f.state.AllocateParseResult<ChoiceParseResult>(*child_result->GetParseResult(),
						                                                           f.child_index, f.start_offset);
					} else {
						completed = MatcherResult::Success();
					}
					break;
				}
				if (f.single_child) {
					break;
				}
				f.child_index++;
				f.child_state.reset();
			}
			if (!f.single_child && f.state.BuildParseResult()) {
				while (f.child_index < f.matcher.matchers.size()) {
					auto &child = f.matcher.matchers[f.child_index].get();
					if (!child.IsStructural() || child.first_set.MightMatch(f.state)) {
						break;
					}
					f.child_index++;
				}
			}
			if (f.child_index >= f.matcher.matchers.size()) {
				break;
			}
			f.child_state.emplace(f.state);
			f.awaiting_child = true;
			next.emplace(MatchInput {f.matcher.matchers[f.child_index].get(), *f.child_state});
			has_child = true;
			break;
		}
		case MatcherType::OPTIONAL: {
			auto &f = static_cast<OptionalMatchFrame &>(frame);
			D_ASSERT(f.awaiting_child == child_result.has_value());
			if (!child_result) {
				f.awaiting_child = true;
				next.emplace(MatchInput {f.matcher.GetChildMatcher(), f.child_state});
				has_child = true;
				break;
			}
			f.awaiting_child = false;
			if (!child_result->IsSuccess()) {
				completed = f.state.AllocateParseResult<OptionalParseResult>();
				break;
			}
			f.state.token_iterator.SetPosition(f.child_state.token_iterator);
			if (child_result->HasParseResult()) {
				completed =
				    f.state.AllocateParseResult<OptionalParseResult>(child_result->GetParseResult(), f.start_offset);
			} else {
				completed = MatcherResult::Success();
			}
			break;
		}
		case MatcherType::REPEAT: {
			auto &f = static_cast<RepeatMatchFrame &>(frame);
			D_ASSERT(f.awaiting_child == child_result.has_value());
			if (child_result) {
				f.awaiting_child = false;
				if (!child_result->IsSuccess()) {
					if (f.matched_once) {
						completed = f.CreateResult();
					}
					break;
				}
				f.matched_once = true;
				if (child_result->HasParseResult()) {
					f.results.emplace_back(*child_result->GetParseResult());
				}
				f.state.token_iterator.SetPosition(f.repeat_state.token_iterator);
				auto current = f.repeat_state.token_iterator.Current();
				if (current && current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
					f.matcher.GetChildMatcher().AddSuggestion(f.state);
					completed = f.CreateResult();
					break;
				}
			}
			f.awaiting_child = true;
			next.emplace(MatchInput {f.matcher.GetChildMatcher(), f.repeat_state});
			has_child = true;
			break;
		}
		default: {
			auto step = static_cast<OpaqueMatchFrame &>(frame).process->Resume(child_result);
			auto child = step.GetChild();
			if (child) {
				next.emplace(*child);
				has_child = true;
			} else {
				completed = step.GetResult();
			}
			break;
		}
		}
		if (has_child) {
			if (SINGLE_STEP) {
				return MatchStep::Child(*next);
			}
			auto &child = *next;
			if (!child.matcher.IsStructural() && child.matcher.IsAtomic()) {
				child_result = stack->ExecuteAtomicMatcher(child);
				continue;
			}
			if (child.state.BuildParseResult()) {
				if (child.matcher.Type() == MatcherType::OPTIONAL && child.matcher.IsStructural() &&
				    !child.matcher.Cast<OptionalMatcher>().GetChildMatcher().first_set.MightMatch(child.state)) {
					child.state.rule = child.matcher.GetRule();
					child_result = child.state.AllocateParseResult<OptionalParseResult>();
					continue;
				}
				if (!child.matcher.first_set.MightMatch(child.state)) {
					child_result = MatcherResult::Failure();
					continue;
				}
			}
			// PushFrame returns the cached result when the packrat cache already holds it
			child_result = stack->PushFrame(child);
			current_frame = *stack->frames.back();
			continue;
		}
		if (SINGLE_STEP) {
			return MatchStep::Complete(completed);
		}
		frame.packrat.StoreResult(frame.input_matcher, frame.state, completed);
		stack->DestroyTopFrame();
		if (stack->frames.empty()) {
			return MatchStep::Complete(completed);
		}
		child_result = completed;
		current_frame = *stack->frames.back();
	}
}

MatchStep MatchStackFrame::Resume(optional<MatcherResult> child_result) {
	// Extensions can drive a built-in process one step at a time through the same implementation.
	return MatchStack::ExecuteInternal<true>(nullptr, *this, child_result);
}

MatchStack::MatchStack() {
}

MatchStack::~MatchStack() {
	Clear();
}

void MatchStack::Clear() {
	// Child processes can reference state owned by their parents.
	while (!frames.empty()) {
		DestroyTopFrame();
	}
}

MatcherResult MatchStack::Execute(MatchInput input) {
	D_ASSERT(frames.empty());
	if (input.matcher.IsAtomic()) {
		return ExecuteAtomicMatcher(input);
	}
	frames.reserve(INITIAL_FRAME_CAPACITY);
	auto immediate = PushFrame(input);
	if (immediate) {
		return *immediate;
	}
	try {
		return ExecuteInternal<false>(this, *frames.back(), nullopt).GetResult();
	} catch (...) {
		Clear();
		throw;
	}
}

arena_ptr<MatchProcess> ListMatcher::StartMatch(MatchState &state) const {
	return state.Make<ListMatchFrame>(*this, state);
}

arena_ptr<MatchProcess> ChoiceMatcher::StartMatch(MatchState &state) const {
	return state.Make<ChoiceMatchFrame>(*this, state);
}

arena_ptr<MatchProcess> OptionalMatcher::StartMatch(MatchState &state) const {
	return state.Make<OptionalMatchFrame>(*this, state);
}

arena_ptr<MatchProcess> RepeatMatcher::StartMatch(MatchState &state) const {
	return state.Make<RepeatMatchFrame>(*this, state);
}

} // namespace duckdb
