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

	MatchResultType Match(MatchState &state) const override {
		for (auto &child_matcher : matchers) {
			MatchState choice_state(state);
			auto child_result = child_matcher.get().Match(choice_state);
			if (child_result != MatchResultType::FAIL) {
				// we matched this child - propagate upwards
				state.token_iterator.SetPosition(choice_state.token_iterator);
				return child_result;
			}
		}
		return MatchResultType::FAIL;
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		optional_idx start_offset;
		if (auto current = state.token_iterator.Current()) {
			start_offset = optional_idx(current->offset);
		}
		for (idx_t i = 0; i < matchers.size(); i++) {
			MatchState choice_state(state);
			auto child_result = matchers[i].get().MatchParseResult(choice_state);
			if (child_result != nullptr) {
				// we matched this child - propagate upwards
				state.token_iterator.SetPosition(choice_state.token_iterator);
				auto result = state.allocator.Allocate(make_uniq<ChoiceParseResult>(*child_result, i, start_offset));
				return result;
			}
		}
		return nullptr;
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
