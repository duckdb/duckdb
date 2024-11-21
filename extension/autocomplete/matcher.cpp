#include "matcher.hpp"

namespace duckdb {

class KeywordMatcher : public Matcher {
public:
	explicit KeywordMatcher(string keyword_p, int32_t score_bonus = 0, char extra_char = '\0') : keyword(std::move(keyword_p)), score_bonus(score_bonus), extra_char(extra_char) {
	}

	MatchResultType Match(MatchState &state) const override {
		auto &token = state.tokens[state.token_index];
		if (StringUtil::CIEquals(keyword, token.text)) {
			// move to the next token
			state.token_index++;
			return MatchResultType::SUCCESS;
		} else {
			return MatchResultType::FAIL;
		}
	}

	SuggestionType AddSuggestion(MatchState &state) const override {
		AutoCompleteCandidate candidate(keyword, score_bonus, CandidateMatchCase::MATCH_CASE);
		candidate.extra_char = extra_char;
		state.suggestions.emplace_back(std::move(candidate));
		return SuggestionType::MANDATORY;
	}

private:
	string keyword;
	int32_t score_bonus;
	char extra_char;
};

class ListMatcher : public Matcher {
public:
	explicit ListMatcher(vector<reference<Matcher>> matchers_p) : matchers(std::move(matchers_p)) {
	}

	MatchResultType Match(MatchState &state) const override {
		MatchState list_state(state);
		for (idx_t child_idx = 0; child_idx < matchers.size(); child_idx++) {
			auto &child_matcher = matchers[child_idx].get();
			if (list_state.token_index >= list_state.tokens.size()) {
				// we exhausted the tokens - push suggestions for the child matcher
				for (; child_idx < matchers.size(); child_idx++) {
					auto suggestion_type = matchers[child_idx].get().AddSuggestion(list_state);
					if (suggestion_type == SuggestionType::MANDATORY) {
						// finished providing suggestions
						break;
					}
				}
				state.token_index = list_state.token_index;
				return MatchResultType::ADDED_SUGGESTION;
			}
			auto match_result = child_matcher.Match(list_state);
			if (match_result != MatchResultType::SUCCESS) {
				// we did not succeed in matching a child - skip
				return match_result;
			}
		}
		// we matched all child matchers - propagate token index upward
		state.token_index = list_state.token_index;
		return MatchResultType::SUCCESS;
	}

	SuggestionType AddSuggestion(MatchState &state) const override {
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

private:
	vector<reference<Matcher>> matchers;
};

class OptionalMatcher : public Matcher {
public:
	explicit OptionalMatcher(Matcher &matcher_p) : matcher(matcher_p) {
	}

	MatchResultType Match(MatchState &state) const override {
		MatchState child_state(state);
		auto child_match = matcher.Match(child_state);
		if (child_match != MatchResultType::SUCCESS) {
			// did not succeed in matching - go back up (but return success anyway)
			return MatchResultType::SUCCESS;
		}
		// propagate the child state upwards
		state.token_index = child_state.token_index;
		return MatchResultType::SUCCESS;
	}

	SuggestionType AddSuggestion(MatchState &state) const override {
		matcher.AddSuggestion(state);
		return SuggestionType::OPTIONAL;
	}

private:
	Matcher &matcher;
};

class ChoiceMatcher : public Matcher {
public:
	explicit ChoiceMatcher(vector<reference<Matcher>> matchers_p) : matchers(std::move(matchers_p)) {
	}

	MatchResultType Match(MatchState &state) const override {
		for (auto &child_matcher : matchers) {
			MatchState choice_state(state);
			auto child_result = child_matcher.get().Match(choice_state);
			if (child_result != MatchResultType::FAIL) {
				// we matched this child - propagate upwards
				state.token_index = choice_state.token_index;
				return child_result;
			}
		}
		return MatchResultType::FAIL;
	}

	SuggestionType AddSuggestion(MatchState &state) const override {
		for (auto &child_matcher : matchers) {
			child_matcher.get().AddSuggestion(state);
		}
		return SuggestionType::MANDATORY;
	}

private:
	vector<reference<Matcher>> matchers;
};

class RepeatMatcher : public Matcher {
public:
	RepeatMatcher(Matcher &element_p, optional_ptr<Matcher> separator_p)
	    : element(element_p), separator(separator_p) {
	}

	MatchResultType Match(MatchState &state) const override {
		MatchState repeat_state(state);
		while (true) {
			// we exhausted the tokens - suggest the element
			if (repeat_state.token_index >= state.tokens.size()) {
				// we exhausted the tokens - suggest the element
				element.AddSuggestion(state);
				return MatchResultType::ADDED_SUGGESTION;
			}

			// first we must match the element
			auto child_match = element.Match(repeat_state);
			if (child_match != MatchResultType::SUCCESS) {
				// match did not succeed - propagate upwards
				return child_match;
			}
			if (!separator) {
				continue;
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

	SuggestionType AddSuggestion(MatchState &state) const override {
		element.AddSuggestion(state);
		return SuggestionType::MANDATORY;
	}

private:
	Matcher &element;
	optional_ptr<Matcher> separator;
};

class VariableMatcher : public Matcher {
public:
	explicit VariableMatcher(SuggestionState suggestion_type) : suggestion_type(suggestion_type) {
	}

	MatchResultType Match(MatchState &state) const override {
		state.token_index++;
		return MatchResultType::SUCCESS;
	}

	SuggestionType AddSuggestion(MatchState &state) const override {
		state.suggestions.emplace_back(suggestion_type);
		return SuggestionType::MANDATORY;
	}

	SuggestionState suggestion_type;
};

Matcher &MatcherAllocator::Allocate(unique_ptr<Matcher> matcher) {
	auto &result = *matcher;
	matchers.push_back(std::move(matcher));
	return result;
}

enum class CachedMatcherType {
	IF_NOT_EXISTS,
	QUALIFIED_TABLE_NAME
};

//! Class for building matchers
class MatcherFactory {
public:
	explicit MatcherFactory(MatcherAllocator &allocator) : allocator(allocator) {}

	Matcher &RootMatcher();

private:
	// Base primitives
	Matcher &Keyword(const string &keyword, int32_t score_bonus= 0, char extra_char = ' ') const;
	Matcher &List(vector<reference<Matcher>> matchers) const;
	Matcher &Choice(vector<reference<Matcher>> matchers) const;
	Matcher &Optional(Matcher &matcher) const;
	Matcher &Repeat(Matcher &matcher, optional_ptr<Matcher> separator) const;
	Matcher &Variable() const;
	Matcher &CatalogName() const;
	Matcher &SchemaName() const;
	Matcher &TypeName() const;
	Matcher &TableName() const;

private:
	// Matchers
	Matcher &OrReplace();
	Matcher &Temporary();
	Matcher &IfNotExists();
	Matcher &CatalogQualification();
	Matcher &SchemaQualification();
	Matcher &QualifiedSchemaName();
	Matcher &QualifiedTableName();
	Matcher &NotNullConstraint();
	Matcher &UniqueConstraint();
	Matcher &PrimaryKeyConstraint();
	Matcher &ColumnConstraint();
	Matcher &ColumnDefinition();
	Matcher &ColumnIdList();
	Matcher &TopLevelPrimaryKeyConstraint();
	Matcher &TopLevelConstraint();
	Matcher &CreateTableColumnList();
	Matcher &CreateTableMatcher();
	Matcher &CreateSchemaMatcher();
	Matcher &FunctionOrMacro();
	Matcher &ColumnReference();
	Matcher &ExpressionDefinition();
	Matcher &ScalarMacroDefinition();
	Matcher &CreateFunctionMatcher();
	Matcher &CreateMatchers();
	Matcher &CreateStatementMatcher();

private:
	optional_ptr<Matcher> CheckCache(CachedMatcherType matcher_type) {
		auto entry = matcher_cache.find(matcher_type);
		if (entry != matcher_cache.end()) {
			// entry was already created - return the entry
			return &entry->second.get();
		}
		return nullptr;
	}

	Matcher &Cache(CachedMatcherType matcher_type, Matcher &matcher) {
		matcher_cache.insert(make_pair(matcher_type, reference<Matcher>(matcher)));
		return matcher;
	}

private:
	MatcherAllocator &allocator;
	unordered_map<CachedMatcherType, reference<Matcher>> matcher_cache;
};

Matcher &MatcherFactory::Keyword(const string &keyword, int32_t score_bonus, char extra_char) const {
	return allocator.Allocate(make_uniq<KeywordMatcher>(keyword, score_bonus, extra_char));
}

Matcher &MatcherFactory::List(vector<reference<Matcher>> matchers) const {
	return allocator.Allocate(make_uniq<ListMatcher>(std::move(matchers)));
}

Matcher &MatcherFactory::Choice(vector<reference<Matcher>> matchers) const {
	return allocator.Allocate(make_uniq<ChoiceMatcher>(std::move(matchers)));
}

Matcher &MatcherFactory::Optional(Matcher &matcher) const {
	return allocator.Allocate(make_uniq<OptionalMatcher>(matcher));
}

Matcher &MatcherFactory::Repeat(Matcher &matcher, optional_ptr<Matcher> separator) const {
	return allocator.Allocate(make_uniq<RepeatMatcher>(matcher, separator));
}

Matcher &MatcherFactory::Variable() const {
	return allocator.Allocate(make_uniq<VariableMatcher>(SuggestionState::SUGGEST_VARIABLE));
}

Matcher &MatcherFactory::CatalogName() const {
	return allocator.Allocate(make_uniq<VariableMatcher>(SuggestionState::SUGGEST_CATALOG_NAME));
}

Matcher &MatcherFactory::SchemaName() const {
	return allocator.Allocate(make_uniq<VariableMatcher>(SuggestionState::SUGGEST_SCHEMA_NAME));
}

Matcher &MatcherFactory::TypeName() const {
	return allocator.Allocate(make_uniq<VariableMatcher>(SuggestionState::SUGGEST_TYPE_NAME));
}

Matcher &MatcherFactory::TableName() const {
	return allocator.Allocate(make_uniq<VariableMatcher>(SuggestionState::SUGGEST_TABLE_NAME));
}

Matcher &MatcherFactory::OrReplace() {
	vector<reference<Matcher>> m;
	m.push_back(Keyword("OR"));
	m.push_back(Keyword("REPLACE"));
	return Optional(List(std::move(m)));
}

Matcher &MatcherFactory::Temporary() {
	vector<reference<Matcher>> m;
	m.push_back(Keyword("TEMP"));
	m.push_back(Keyword("TEMPORARY"));
	return Optional(Choice(std::move(m)));
}

Matcher &MatcherFactory::IfNotExists() {
	auto cache_type = CachedMatcherType::IF_NOT_EXISTS;
	auto cache_entry = CheckCache(cache_type);
	if (cache_entry) {
		return *cache_entry;
	}
	vector<reference<Matcher>> m;
	m.push_back(Keyword("IF"));
	m.push_back(Keyword("NOT"));
	m.push_back(Keyword("EXISTS"));
	return Cache(cache_type, Optional(List(std::move(m))));
}

Matcher &MatcherFactory::CatalogQualification() {
	vector<reference<Matcher>> m;
	m.push_back(CatalogName());
	m.push_back(Keyword(".", 0, '\0'));
	return List(std::move(m));
}

Matcher &MatcherFactory::SchemaQualification() {
	vector<reference<Matcher>> m;
	m.push_back(SchemaName());
	m.push_back(Keyword(".", 0, '\0'));
	return List(std::move(m));
}

Matcher &MatcherFactory::QualifiedSchemaName() {
	vector<reference<Matcher>> m;
	m.push_back(Optional(CatalogQualification()));
	m.push_back(Variable());
	return List(std::move(m));
}

Matcher &MatcherFactory::QualifiedTableName() {
	auto cache_type = CachedMatcherType::QUALIFIED_TABLE_NAME;
	auto cache_entry = CheckCache(cache_type);
	if (cache_entry) {
		return *cache_entry;
	}
	vector<reference<Matcher>> m;
	m.push_back(Optional(CatalogQualification()));
	m.push_back(Optional(SchemaQualification()));
	m.push_back(Variable());
	return Cache(cache_type, List(std::move(m)));
}

Matcher &MatcherFactory::NotNullConstraint() {
	vector<reference<Matcher>> m;
	m.push_back(Keyword("NOT"));
	m.push_back(Keyword("NULL", 0, '\0'));
	return List(std::move(m));
}

Matcher &MatcherFactory::UniqueConstraint() {
	return Keyword("UNIQUE", 0, '\0');
}

Matcher &MatcherFactory::PrimaryKeyConstraint() {
	vector<reference<Matcher>> m;
	m.push_back(Keyword("PRIMARY"));
	m.push_back(Keyword("KEY", 0, '\0'));
	return List(std::move(m));
}

Matcher &MatcherFactory::ColumnConstraint() {
	vector<reference<Matcher>> m;
	m.push_back(NotNullConstraint());
	m.push_back(UniqueConstraint());
	m.push_back(PrimaryKeyConstraint());
	return Repeat(Choice(std::move(m)), nullptr);
}

Matcher &MatcherFactory::ColumnDefinition() {
	vector<reference<Matcher>> m;
	m.push_back(Variable());
	m.push_back(TypeName());
	m.push_back(Optional(ColumnConstraint()));
	return List(std::move(m));
}

Matcher &MatcherFactory::ColumnIdList() {
	vector<reference<Matcher>> m;
	m.push_back(Keyword("("));
	m.push_back(Repeat(Variable(), Keyword(",")));
	m.push_back(Keyword(")"));
	return List(std::move(m));
}

Matcher &MatcherFactory::TopLevelPrimaryKeyConstraint() {
	vector<reference<Matcher>> m;
	m.push_back(Keyword("PRIMARY"));
	m.push_back(Keyword("KEY", 0, '\0'));
	m.push_back(ColumnIdList());
	return List(std::move(m));
}

Matcher &MatcherFactory::TopLevelConstraint() {
	vector<reference<Matcher>> m;
	m.push_back(TopLevelPrimaryKeyConstraint());
	return Choice(std::move(m));
}

Matcher &MatcherFactory::CreateTableColumnList() {
	vector<reference<Matcher>> m;
	m.push_back(ColumnDefinition());
	m.push_back(TopLevelConstraint());
	return Repeat(Choice(std::move(m)), Keyword(","));
}

Matcher &MatcherFactory::CreateTableMatcher() {
	vector<reference<Matcher>> m;
	m.push_back(Keyword("TABLE", 1));
	m.push_back(IfNotExists());
	m.push_back(QualifiedTableName());
	m.push_back(Keyword("(", 0, '\0'));
	m.push_back(CreateTableColumnList());
	m.push_back(Keyword(";"));
	return List(std::move(m));
}

Matcher &MatcherFactory::CreateSchemaMatcher() {
	vector<reference<Matcher>> m;
	m.push_back(Keyword("SCHEMA", 1));
	m.push_back(IfNotExists());
	m.push_back(QualifiedSchemaName());
	m.push_back(Keyword(";"));
	return List(std::move(m));
}

Matcher &MatcherFactory::FunctionOrMacro() {
	vector<reference<Matcher>> m;
	m.push_back(Keyword("FUNCTION", 1));
	m.push_back(Keyword("MACRO", 1));
	return Choice(std::move(m));
}

Matcher &MatcherFactory::ColumnReference() {
	return Variable();
}

Matcher &MatcherFactory::ExpressionDefinition() {
	vector<reference<Matcher>> m;
	m.push_back(ColumnReference());
	return Choice(std::move(m));
}

Matcher &MatcherFactory::ScalarMacroDefinition() {
	vector<reference<Matcher>> m;
	m.push_back(ColumnIdList());
	m.push_back(Keyword("AS"));
	m.push_back(ExpressionDefinition());
	return Choice(std::move(m));
}

Matcher &MatcherFactory::CreateFunctionMatcher() {
	vector<reference<Matcher>> m;
	m.push_back(FunctionOrMacro());
	m.push_back(IfNotExists());
	m.push_back(QualifiedTableName());
	m.push_back(Repeat(ScalarMacroDefinition(), Keyword(",")));
	return List(std::move(m));
}

Matcher &MatcherFactory::CreateMatchers() {
	vector<reference<Matcher>> m;
	m.push_back(CreateTableMatcher());
	m.push_back(CreateSchemaMatcher());
	m.push_back(CreateFunctionMatcher());
	return Choice(std::move(m));
}

Matcher &MatcherFactory::CreateStatementMatcher() {
	vector<reference<Matcher>> m;
	m.push_back(Keyword("CREATE"));
	m.push_back(OrReplace());
	m.push_back(Temporary());
	m.push_back(CreateMatchers());
	return List(std::move(m));
}

Matcher &MatcherFactory::RootMatcher() {
	vector<reference<Matcher>> m;
	m.push_back(CreateStatementMatcher());
	return Choice(std::move(m));
}

Matcher &Matcher::RootMatcher(MatcherAllocator &allocator) {
	MatcherFactory factory(allocator);
	return factory.RootMatcher();
}

} // namespace duckdb
