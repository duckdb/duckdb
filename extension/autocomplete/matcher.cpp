#include "matcher.hpp"

namespace duckdb {

class KeywordMatcher : public Matcher {
public:
	KeywordMatcher(string keyword_p) : keyword(std::move(keyword_p)) {
	}

	MatchResultType Match(MatchState &state) override {
		auto &token = state.tokens[state.token_index];
		if (StringUtil::CIEquals(keyword, token.text)) {
			// move to the next token
			state.token_index++;
			return MatchResultType::SUCCESS;
		} else {
			return MatchResultType::FAIL;
		}
	}

	SuggestionType AddSuggestion(MatchState &state) override {
		state.suggestions.push_back(keyword);
		return SuggestionType::MANDATORY;
	}

private:
	string keyword;
};

class ListMatcher : public Matcher {
public:
	ListMatcher(vector<unique_ptr<Matcher>> matchers_p) : matchers(std::move(matchers_p)) {
	}

	MatchResultType Match(MatchState &state) override {
		MatchState list_state(state);
		for (idx_t child_idx = 0; child_idx < matchers.size(); child_idx++) {
			auto &child_matcher = matchers[child_idx];
			if (list_state.token_index >= list_state.tokens.size()) {
				// we exhausted the tokens - push suggestions for the child matcher
				for (; child_idx < matchers.size(); child_idx++) {
					auto suggestion_type = matchers[child_idx]->AddSuggestion(list_state);
					if (suggestion_type == SuggestionType::MANDATORY) {
						// finished providing suggestions
						break;
					}
				}
				state.token_index = list_state.token_index;
				return MatchResultType::ADDED_SUGGESTION;
			}
			auto match_result = child_matcher->Match(list_state);
			if (match_result != MatchResultType::SUCCESS) {
				// we did not succeed in matching a child - skip
				return match_result;
			}
		}
		// we matched all child matchers - propagate token index upward
		state.token_index = list_state.token_index;
		return MatchResultType::SUCCESS;
	}

	SuggestionType AddSuggestion(MatchState &state) override {
		for (auto &matcher : matchers) {
			auto suggestion_result = matcher->AddSuggestion(state);
			if (suggestion_result == SuggestionType::MANDATORY) {
				// we must match this suggestion before continuing
				return SuggestionType::MANDATORY;
			}
		}
		// all child suggestions were optional - the entire list is optional
		return SuggestionType::OPTIONAL;
	}

private:
	vector<unique_ptr<Matcher>> matchers;
};

class OptionalMatcher : public Matcher {
public:
	OptionalMatcher(unique_ptr<Matcher> matcher_p) : matcher(std::move(matcher_p)) {
	}

	MatchResultType Match(MatchState &state) override {
		MatchState child_state(state);
		auto child_match = matcher->Match(child_state);
		if (child_match != MatchResultType::SUCCESS) {
			// did not succeed in matching - go back up (but return success anyway)
			return MatchResultType::SUCCESS;
		}
		// propagate the child state upwards
		state.token_index = child_state.token_index;
		return MatchResultType::SUCCESS;
	}

	SuggestionType AddSuggestion(MatchState &state) override {
		matcher->AddSuggestion(state);
		return SuggestionType::OPTIONAL;
	}

private:
	unique_ptr<Matcher> matcher;
};

class ChoiceMatcher : public Matcher {
public:
	ChoiceMatcher(vector<unique_ptr<Matcher>> matchers_p) : matchers(std::move(matchers_p)) {
	}

	MatchResultType Match(MatchState &state) override {
		for (auto &child_matcher : matchers) {
			MatchState choice_state(state);
			auto child_result = child_matcher->Match(choice_state);
			if (child_result != MatchResultType::FAIL) {
				// we matched this child - propagate upwards
				state.token_index = choice_state.token_index;
				return child_result;
			}
		}
		return MatchResultType::FAIL;
	}

	SuggestionType AddSuggestion(MatchState &state) override {
		for (auto &child_matcher : matchers) {
			child_matcher->AddSuggestion(state);
		}
		return SuggestionType::MANDATORY;
	}

private:
	vector<unique_ptr<Matcher>> matchers;
};

class RepeatMatcher : public Matcher {
public:
	RepeatMatcher(unique_ptr<Matcher> element_p, unique_ptr<Matcher> separator_p)
	    : element(std::move(element_p)), separator(std::move(separator_p)) {
	}

	MatchResultType Match(MatchState &state) override {
		MatchState repeat_state(state);
		while (true) {
			// we exhausted the tokens - suggest the element
			if (repeat_state.token_index >= state.tokens.size()) {
				// we exhausted the tokens - suggest the element
				element->AddSuggestion(state);
				return MatchResultType::ADDED_SUGGESTION;
			}

			// first we must match the element
			auto child_match = element->Match(repeat_state);
			if (child_match != MatchResultType::SUCCESS) {
				// match did not succeed - propagate upwards
				return child_match;
			}
			// succeeded in matching the child
			// now we optionally match the separator
			idx_t successful_token = repeat_state.token_index;

			if (successful_token >= state.tokens.size()) {
				// we exhausted the tokens - suggest the separator
				separator->AddSuggestion(state);
				return MatchResultType::ADDED_SUGGESTION;
			}

			auto repeat_match = separator->Match(repeat_state);
			if (repeat_match == MatchResultType::SUCCESS) {
				// we suggested in matching the separator - match the element again
				continue;
			}
			// we did not succeed in matching the separator - this means the repetition has ended
			state.token_index = successful_token;
			return MatchResultType::SUCCESS;
		}
	}

	SuggestionType AddSuggestion(MatchState &state) override {
		element->AddSuggestion(state);
		return SuggestionType::MANDATORY;
	}

private:
	unique_ptr<Matcher> element;
	unique_ptr<Matcher> separator;
};

class VariableMatcher : public Matcher {
public:
	VariableMatcher(SuggestionState suggestion_type) : suggestion_type(suggestion_type) {
	}

	MatchResultType Match(MatchState &state) override {
		state.token_index++;
		return MatchResultType::SUCCESS;
	}

	SuggestionType AddSuggestion(MatchState &state) override {
		state.suggestions.emplace_back(suggestion_type);
		return SuggestionType::MANDATORY;
	}

	SuggestionState suggestion_type;
};

unique_ptr<Matcher> Matcher::Keyword(const string &keyword) {
	return make_uniq<KeywordMatcher>(keyword);
}

unique_ptr<Matcher> Matcher::List(vector<unique_ptr<Matcher>> matchers) {
	return make_uniq<ListMatcher>(std::move(matchers));
}

unique_ptr<Matcher> Matcher::Choice(vector<unique_ptr<Matcher>> matchers) {
	return make_uniq<ChoiceMatcher>(std::move(matchers));
}

unique_ptr<Matcher> Matcher::Optional(unique_ptr<Matcher> matcher) {
	return make_uniq<OptionalMatcher>(std::move(matcher));
}

unique_ptr<Matcher> Matcher::Repeat(unique_ptr<Matcher> matcher, unique_ptr<Matcher> separator) {
	return make_uniq<RepeatMatcher>(std::move(matcher), std::move(separator));
}

unique_ptr<Matcher> Matcher::Variable() {
	return make_uniq<VariableMatcher>(SuggestionState::SUGGEST_VARIABLE);
}

unique_ptr<Matcher> Matcher::CatalogName() {
	return make_uniq<VariableMatcher>(SuggestionState::SUGGEST_CATALOG_NAME);
}

unique_ptr<Matcher> Matcher::SchemaName() {
	return make_uniq<VariableMatcher>(SuggestionState::SUGGEST_SCHEMA_NAME);
}

unique_ptr<Matcher> Matcher::TypeName() {
	return make_uniq<VariableMatcher>(SuggestionState::SUGGEST_TYPE_NAME);
}

unique_ptr<Matcher> Matcher::TableName() {
	return make_uniq<VariableMatcher>(SuggestionState::SUGGEST_TABLE_NAME);
}

unique_ptr<Matcher> TemporaryOrReplace() {
	vector<unique_ptr<Matcher>> m;
	m.push_back(Matcher::Keyword("TEMP"));
	m.push_back(Matcher::Keyword("TEMPORARY"));
	return Matcher::Optional(Matcher::Choice(std::move(m)));
}

unique_ptr<Matcher> IfNotExists() {
	vector<unique_ptr<Matcher>> m;
	m.push_back(Matcher::Keyword("IF"));
	m.push_back(Matcher::Keyword("NOT"));
	m.push_back(Matcher::Keyword("EXISTS"));
	return Matcher::Optional(Matcher::List(std::move(m)));
}

unique_ptr<Matcher> CatalogQualification() {
	vector<unique_ptr<Matcher>> m;
	m.push_back(Matcher::CatalogName());
	m.push_back(Matcher::Keyword("."));
	return Matcher::List(std::move(m));
}

unique_ptr<Matcher> SchemaQualification() {
	vector<unique_ptr<Matcher>> m;
	m.push_back(Matcher::SchemaName());
	m.push_back(Matcher::Keyword("."));
	return Matcher::List(std::move(m));
}

unique_ptr<Matcher> QualifiedTableName() {
	vector<unique_ptr<Matcher>> m;
	m.push_back(Matcher::Optional(CatalogQualification()));
	m.push_back(Matcher::Optional(SchemaQualification()));
	m.push_back(Matcher::Variable());
	return Matcher::List(std::move(m));
}

unique_ptr<Matcher> CreateTableColumnList() {
	vector<unique_ptr<Matcher>> m;
	m.push_back(Matcher::Variable());
	m.push_back(Matcher::TypeName());
	return Matcher::Repeat(Matcher::List(std::move(m)), Matcher::Keyword(","));
}

unique_ptr<Matcher> KeywordCreateMatcher() {
	vector<unique_ptr<Matcher>> m;
	m.push_back(Matcher::Keyword("CREATE"));
	m.push_back(TemporaryOrReplace());
	m.push_back(Matcher::Keyword("TABLE"));
	m.push_back(IfNotExists());
	m.push_back(QualifiedTableName());
	m.push_back(Matcher::Keyword("("));
	m.push_back(CreateTableColumnList());
	m.push_back(Matcher::Keyword(";"));
	return Matcher::List(std::move(m));
}

unique_ptr<Matcher> Matcher::RootMatcher() {
	vector<unique_ptr<Matcher>> m;
	m.push_back(KeywordCreateMatcher());
	return Matcher::Choice(std::move(m));
}

} // namespace duckdb
