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

	MatcherResult MatchParseResultInternal(MatchState &state) const override {
		optional_idx start_offset;
		if (auto current = state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
		for (idx_t i = 0; i < matchers.size(); i++) {
			MatchState choice_state(state);
			auto child_result = matchers[i].get().MatchParseResult(choice_state);
			if (child_result.IsSuccess()) {
				// we matched this child - propagate upwards
				state.token_iterator.SetPosition(choice_state.token_iterator);
				if (!child_result.HasParseResult()) {
					return MatcherResult::Success();
				}
				return state.AllocateParseResult<ChoiceParseResult>(*child_result.GetParseResult(), i, start_offset);
			}
		}
		return MatcherResult::Failure();
	}

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
