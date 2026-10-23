#pragma once

#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

class ListMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::LIST;

public:
	ListMatcher() : Matcher(TYPE) {
	}
	explicit ListMatcher(vector<reference<Matcher>> matchers_p) : Matcher(TYPE), matchers(std::move(matchers_p)) {
	}

	DUCKDB_API arena_ptr<MatchProcess> StartMatch(MatchState &state) const override;
	//! Prepare after matcher links and flags are final; unprepared lists use ordinary execution.
	DUCKDB_API void InitializeListChain();

	optional_ptr<const ListMatcher> GetChainChild() const {
		if (!first_child_is_chainable) {
			return nullptr;
		}
		D_ASSERT(IsStructural() && !suppress_suggestions && !matchers.empty());
		auto &child = matchers[0].get();
		D_ASSERT(child.IsStructural() && child.Type() == MatcherType::LIST && !child.IsPackratMemoized());
		auto &list = static_cast<const ListMatcher &>(child);
		D_ASSERT(!list.suppress_suggestions);
		return list;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		if (suppress_suggestions) {
			return SuggestionType::OPTIONAL;
		}
		for (auto &matcher : matchers) {
			auto suggestion_result = matcher.get().AddSuggestion(state);
			if (suggestion_result == SuggestionType::MANDATORY) {
				// we must match this suggestion before continuing
				return SuggestionType::MANDATORY;
			}
		}
		// all child suggestions were optional - the entire list is optional
		return SuggestionType::OPTIONAL;
	}

	string ToString() const override {
		string result = "";
		for (auto &matcher : matchers) {
			if (!result.empty()) {
				result += " ";
			}
			result += matcher.get().GetPrintName();
		}
		return "(" + result + ")";
	}

public:
	vector<reference<Matcher>> matchers;
	//! If true, this matcher will not contribute autocomplete suggestions (used for rules like ExpressionStatement)
	bool suppress_suggestions = false;

private:
	bool first_child_is_chainable = false;
};

} // namespace duckdb
