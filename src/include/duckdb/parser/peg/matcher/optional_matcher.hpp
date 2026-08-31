#pragma once

#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class OptionalMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::OPTIONAL;

public:
	explicit OptionalMatcher(Matcher &matcher_p) : Matcher(TYPE), matcher(matcher_p) {
	}

	MatchResultType Match(MatchState &state) const override {
		MatchState child_state(state);
		auto child_match = matcher.Match(child_state);
		if (child_match == MatchResultType::FAIL) {
			// did not succeed in matching - go back up (but return success anyway)
			return MatchResultType::SUCCESS;
		}
		// propagate the child state upwards
		state.token_iterator.SetPosition(child_state.token_iterator);
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		MatchState child_state(state);
		optional_idx start_offset;
		if (auto current = child_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
		auto child_match = matcher.MatchParseResult(child_state);
		if (child_match == nullptr) {
			// did not succeed in matching - go back up (simply return a nullptr)
			return state.allocator.Allocate(make_uniq<OptionalParseResult>());
		}
		// propagate the child state upwards
		state.token_iterator.SetPosition(child_state.token_iterator);
		return state.allocator.Allocate(make_uniq<OptionalParseResult>(child_match, start_offset));
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		matcher.AddSuggestion(state);
		return SuggestionType::OPTIONAL;
	}

	string ToString() const override {
		return matcher.GetName() + "?";
	}

private:
	Matcher &matcher;
};

} // namespace duckdb
