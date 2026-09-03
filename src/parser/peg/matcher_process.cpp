#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/matcher/choice_matcher.hpp"
#include "duckdb/parser/peg/matcher/list_matcher.hpp"
#include "duckdb/parser/peg/matcher/optional_matcher.hpp"
#include "duckdb/parser/peg/matcher/repeat_matcher.hpp"

namespace duckdb {

MatchStep MatchStep::Child(MatchInput input) {
	return MatchStep(input, nullopt);
}

MatchStep MatchStep::Complete(MatcherResult result) {
	return MatchStep(nullopt, result);
}

optional<MatchInput> MatchStep::GetChild() {
	return child;
}

MatcherResult MatchStep::GetResult() const {
	D_ASSERT(!child);
	D_ASSERT(result);
	return *result;
}

class AtomicMatchProcess : public MatchProcess {
public:
	AtomicMatchProcess(const AtomicMatcher &matcher_p, MatchState &state_p) : matcher(matcher_p), state(state_p) {
	}

	MatchStep Resume(optional<MatcherResult> child_result) override {
		D_ASSERT(!child_result);
		D_ASSERT(!completed);
		completed = true;
		return MatchStep::Complete(matcher.MatchAtomic(state));
	}

private:
	const AtomicMatcher &matcher;
	MatchState &state;
	bool completed = false;
};

unique_ptr<MatchProcess> AtomicMatcher::StartMatch(MatchState &state) const {
	return make_uniq<AtomicMatchProcess>(*this, state);
}

class ListMatchProcess : public MatchProcess {
public:
	ListMatchProcess(const ListMatcher &matcher_p, MatchState &state_p)
	    : matcher(matcher_p), state(state_p), list_state(state_p) {
		saved_suggestion_size = matcher.suppress_suggestions ? list_state.suggestions.size() : 0;
		if (auto current = list_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
	}

	MatchStep Resume(optional<MatcherResult> child_result) override {
		D_ASSERT(awaiting_child == child_result.has_value());
		if (child_result) {
			awaiting_child = false;
			if (!child_result->IsSuccess()) {
				DiscardSuggestions();
				return MatchStep::Complete(MatcherResult::Failure());
			}
			if (child_result->HasParseResult()) {
				results.push_back(*child_result->GetParseResult());
			}
			child_index++;
		}
		while (child_index < matcher.matchers.size()) {
			auto current = list_state.token_iterator.Current();
			bool at_autocomplete_cursor = current && current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE;
			if (!at_autocomplete_cursor) {
				awaiting_child = true;
				return MatchStep::Child({matcher.matchers[child_index].get(), list_state});
			}
			if (matcher.suppress_suggestions) {
				DiscardSuggestions();
				return MatchStep::Complete(MatcherResult::Failure());
			}
			if (matcher.matchers[child_index].get().AddSuggestion(list_state) == SuggestionType::OPTIONAL) {
				child_index++;
				continue;
			}
			state.token_iterator.SetPosition(list_state.token_iterator);
			return MatchStep::Complete(MatcherResult::Failure());
		}
		state.token_iterator.SetPosition(list_state.token_iterator);
		DiscardSuggestions();
		auto list_name = matcher.HasName() ? matcher.GetName() : string();
		return MatchStep::Complete(
		    state.AllocateParseResult<ListParseResult>(std::move(results), std::move(list_name), start_offset));
	}

private:
	void DiscardSuggestions() {
		if (!matcher.suppress_suggestions) {
			return;
		}
		list_state.suggestions.erase(list_state.suggestions.begin() + NumericCast<int64_t>(saved_suggestion_size),
		                             list_state.suggestions.end());
	}

private:
	const ListMatcher &matcher;
	MatchState &state;
	MatchState list_state;
	vector<reference<ParseResult>> results;
	idx_t child_index = 0;
	idx_t saved_suggestion_size = 0;
	optional_idx start_offset;
	bool awaiting_child = false;
};

unique_ptr<MatchProcess> ListMatcher::StartMatch(MatchState &state) const {
	return make_uniq<ListMatchProcess>(*this, state);
}

class ChoiceMatchProcess : public MatchProcess {
public:
	ChoiceMatchProcess(const ChoiceMatcher &matcher_p, MatchState &state_p) : matcher(matcher_p), state(state_p) {
		if (auto current = state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
	}

	MatchStep Resume(optional<MatcherResult> child_result) override {
		D_ASSERT(awaiting_child == child_result.has_value());
		if (child_result) {
			awaiting_child = false;
			D_ASSERT(child_state);
			if (child_result->IsSuccess()) {
				state.token_iterator.SetPosition(child_state->token_iterator);
				if (!child_result->HasParseResult()) {
					return MatchStep::Complete(MatcherResult::Success());
				}
				return MatchStep::Complete(state.AllocateParseResult<ChoiceParseResult>(*child_result->GetParseResult(),
				                                                                        child_index, start_offset));
			}
			child_index++;
			child_state.reset();
		}
		if (child_index >= matcher.matchers.size()) {
			return MatchStep::Complete(MatcherResult::Failure());
		}
		child_state = make_uniq<MatchState>(state);
		awaiting_child = true;
		return MatchStep::Child({matcher.matchers[child_index].get(), *child_state});
	}

private:
	const ChoiceMatcher &matcher;
	MatchState &state;
	unique_ptr<MatchState> child_state;
	idx_t child_index = 0;
	optional_idx start_offset;
	bool awaiting_child = false;
};

unique_ptr<MatchProcess> ChoiceMatcher::StartMatch(MatchState &state) const {
	return make_uniq<ChoiceMatchProcess>(*this, state);
}

class OptionalMatchProcess : public MatchProcess {
public:
	OptionalMatchProcess(const OptionalMatcher &matcher_p, MatchState &state_p)
	    : matcher(matcher_p), state(state_p), child_state(state_p) {
		if (auto current = child_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
	}

	MatchStep Resume(optional<MatcherResult> child_result) override {
		D_ASSERT(awaiting_child == child_result.has_value());
		if (!child_result) {
			awaiting_child = true;
			return MatchStep::Child({matcher.GetChildMatcher(), child_state});
		}
		awaiting_child = false;
		if (!child_result->IsSuccess()) {
			return MatchStep::Complete(state.AllocateParseResult<OptionalParseResult>());
		}
		state.token_iterator.SetPosition(child_state.token_iterator);
		if (!child_result->HasParseResult()) {
			return MatchStep::Complete(MatcherResult::Success());
		}
		return MatchStep::Complete(
		    state.AllocateParseResult<OptionalParseResult>(child_result->GetParseResult(), start_offset));
	}

private:
	const OptionalMatcher &matcher;
	MatchState &state;
	MatchState child_state;
	optional_idx start_offset;
	bool awaiting_child = false;
};

unique_ptr<MatchProcess> OptionalMatcher::StartMatch(MatchState &state) const {
	return make_uniq<OptionalMatchProcess>(*this, state);
}

class RepeatMatchProcess : public MatchProcess {
public:
	RepeatMatchProcess(const RepeatMatcher &matcher_p, MatchState &state_p)
	    : matcher(matcher_p), state(state_p), repeat_state(state_p) {
		if (auto current = repeat_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
	}

	MatchStep Resume(optional<MatcherResult> child_result) override {
		D_ASSERT(awaiting_child == child_result.has_value());
		if (child_result) {
			awaiting_child = false;
			if (!child_result->IsSuccess()) {
				if (!matched_once) {
					return MatchStep::Complete(MatcherResult::Failure());
				}
				return MatchStep::Complete(CreateResult());
			}
			matched_once = true;
			if (child_result->HasParseResult()) {
				results.push_back(*child_result->GetParseResult());
			}
			state.token_iterator.SetPosition(repeat_state.token_iterator);
			auto current = repeat_state.token_iterator.Current();
			if (current && current->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
				matcher.GetChildMatcher().AddSuggestion(state);
				return MatchStep::Complete(CreateResult());
			}
		}
		awaiting_child = true;
		return MatchStep::Child({matcher.GetChildMatcher(), repeat_state});
	}

private:
	MatcherResult CreateResult() {
		return state.AllocateParseResult<RepeatParseResult>(std::move(results), start_offset);
	}

private:
	const RepeatMatcher &matcher;
	MatchState &state;
	MatchState repeat_state;
	vector<reference<ParseResult>> results;
	bool matched_once = false;
	optional_idx start_offset;
	bool awaiting_child = false;
};

unique_ptr<MatchProcess> RepeatMatcher::StartMatch(MatchState &state) const {
	return make_uniq<RepeatMatchProcess>(*this, state);
}

} // namespace duckdb
