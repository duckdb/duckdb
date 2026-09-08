#pragma once

#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class ChoiceMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::CHOICE;

public:
	ChoiceMatcher() : Matcher(TYPE) {
	}
	explicit ChoiceMatcher(vector<reference<Matcher>> &&matchers_p) : Matcher(TYPE), matchers(std::move(matchers_p)) {
	}

	unique_ptr<MatchProcess> StartMatch(MatchState &state) const override;

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		for (auto &child_matcher : matchers) {
			child_matcher.get().AddSuggestion(state);
		}
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		string result = "";
		for (auto &matcher : matchers) {
			if (!result.empty()) {
				result += " / ";
			}
			result += matcher.get().GetName();
		}
		return result;
	}

public:
	vector<reference<Matcher>> matchers;
};

} // namespace duckdb
