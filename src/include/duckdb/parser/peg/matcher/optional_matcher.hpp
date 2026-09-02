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

	unique_ptr<MatchContinuation> StartMatch(MatchState &state) const override;

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
