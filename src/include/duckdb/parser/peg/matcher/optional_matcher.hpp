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

	MatcherResult MatchParseResultInternal(MatchState &state) const override {
		MatchState child_state(state);
		optional_idx start_offset;
		if (auto current = child_state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
		auto child_match = matcher.MatchParseResult(child_state);
		if (!child_match.IsSuccess()) {
			// The optional child did not match, so succeed without advancing.
			return state.AllocateParseResult<OptionalParseResult>();
		}
		// propagate the child state upwards
		state.token_iterator.SetPosition(child_state.token_iterator);
		if (!child_match.HasParseResult()) {
			return MatcherResult::Success();
		}
		return state.AllocateParseResult<OptionalParseResult>(child_match.GetParseResult(), start_offset);
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		matcher.AddSuggestion(state);
		return SuggestionType::OPTIONAL;
	}

	string ToString() const override {
		return matcher.GetName() + "?";
	}
	const Matcher &GetChildMatcher() const {
		return matcher;
	}

private:
	Matcher &matcher;
};

} // namespace duckdb
