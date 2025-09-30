#include "matcher.hpp"

// uncomment to dynamically read the PEG parser from a file instead of compiling it in (useful for testing)
// #define PEG_PARSER_SOURCE_FILE "extension/autocomplete/include/inlined_grammar.gram"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "keyword_helper.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "tokenizer.hpp"
#ifdef PEG_PARSER_SOURCE_FILE
#include <fstream>
#else
#include "inlined_grammar.hpp"
#endif

namespace duckdb {
struct PEGParser;

SuggestionType Matcher::AddSuggestion(MatchState &state) const {
	auto entry = state.added_suggestions.find(*this);
	if (entry != state.added_suggestions.end()) {
		return SuggestionType::MANDATORY;
	}
	state.added_suggestions.insert(*this);
	return AddSuggestionInternal(state);
}

string Matcher::GetName() const {
	if (name.empty()) {
		return ToString();
	}
	return name;
}

void Matcher::Print() const {
	Printer::Print(ToString());
}

void MatchState::AddSuggestion(MatcherSuggestion suggestion) {
	suggestions.push_back(std::move(suggestion));
}

class KeywordMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::KEYWORD;

public:
	explicit KeywordMatcher(string keyword_p, int32_t score_bonus = 0, char extra_char = '\0')
	    : Matcher(TYPE), keyword(std::move(keyword_p)), score_bonus(score_bonus), extra_char(extra_char) {
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

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		AutoCompleteCandidate candidate(keyword, score_bonus, CandidateType::KEYWORD);
		candidate.extra_char = extra_char;
		state.AddSuggestion(MatcherSuggestion(std::move(candidate)));
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "'" + keyword + "'";
	}

private:
	string keyword;
	int32_t score_bonus;
	char extra_char;
};

class ListMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::LIST;

public:
	ListMatcher() : Matcher(TYPE) {
	}
	explicit ListMatcher(vector<reference<Matcher>> matchers_p) : Matcher(TYPE), matchers(std::move(matchers_p)) {
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
				if (child_idx == matchers.size()) {
					// we managed to provide suggestions for all tokens
					// that means all other tokens were optional - i.e. we succeeded in matching them
					return MatchResultType::SUCCESS;
				}
				return MatchResultType::FAIL;
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

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
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
			result += matcher.get().GetName();
		}
		return "(" + result + ")";
	}

public:
	vector<reference<Matcher>> matchers;
};

class OptionalMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::OPTIONAL;

public:
	explicit OptionalMatcher(Matcher &matcher_p) : Matcher(TYPE), matcher(matcher_p) {
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

class ChoiceMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::CHOICE;

public:
	ChoiceMatcher() : Matcher(TYPE) {
	}
	explicit ChoiceMatcher(vector<reference<Matcher>> matchers_p) : Matcher(TYPE), matchers(std::move(matchers_p)) {
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

class RepeatMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::REPEAT;

public:
	explicit RepeatMatcher(Matcher &element_p) : Matcher(TYPE), element(element_p) {
	}

	MatchResultType Match(MatchState &state) const override {
		MatchState repeat_state(state);

		// first we must match the element
		auto child_match = element.Match(repeat_state);
		if (child_match != MatchResultType::SUCCESS) {
			// match did not succeed - propagate upwards
			return child_match;
		}
		// we have matched (at least) once - so this is always a success
		// now we can keep on repeating the matching (optionally)
		while (true) {
			// update the token index we propagate upwards
			state.token_index = repeat_state.token_index;

			// check if we have tokens left
			if (repeat_state.token_index >= state.tokens.size()) {
				// we exhausted the tokens - suggest the element
				element.AddSuggestion(state);
				return MatchResultType::SUCCESS;
			}

			// now match the element again
			child_match = element.Match(repeat_state);
			if (child_match != MatchResultType::SUCCESS) {
				// if we did not succeed we are done matching
				return MatchResultType::SUCCESS;
			}
		}
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		element.AddSuggestion(state);
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return element.GetName() + "*";
	}

private:
	Matcher &element;
};

class IdentifierMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::VARIABLE;

public:
	explicit IdentifierMatcher(SuggestionState suggestion_type) : Matcher(TYPE), suggestion_type(suggestion_type) {
	}

	bool IsIdentifier(const string &text) const {
		if (text.empty()) {
			return false;
		}
		if (text.front() == '\'' && text.back() == '\'' && SupportsStringLiteral()) {
			return true;
		}
		if (text.front() == '"' && text.back() == '"') {
			return true;
		}
		return BaseTokenizer::CharacterIsKeyword(text[0]);
	}

	MatchResultType Match(MatchState &state) const override {
		// variable matchers match anything except for reserved keywords
		auto &token_text = state.tokens[state.token_index].text;
		const auto &keyword_helper = KeywordHelper::Instance();
		switch (suggestion_type) {
		case SuggestionState::SUGGEST_TYPE_NAME:
			if (keyword_helper.KeywordCategoryType(token_text, KeywordCategory::KEYWORD_RESERVED) ||
			    keyword_helper.KeywordCategoryType(token_text, GetBannedCategory())) {
				return MatchResultType::FAIL;
			}
			break;
		default: {
			const auto banned_category = GetBannedCategory();
			const auto allowed_override_category = banned_category == KeywordCategory::KEYWORD_COL_NAME
			                                           ? KeywordCategory::KEYWORD_TYPE_FUNC
			                                           : KeywordCategory::KEYWORD_COL_NAME;

			const bool is_reserved = keyword_helper.KeywordCategoryType(token_text, KeywordCategory::KEYWORD_RESERVED);
			const bool has_extra_banned_category = keyword_helper.KeywordCategoryType(token_text, banned_category);
			const bool has_banned_flag = is_reserved || has_extra_banned_category;

			const bool is_unreserved =
			    keyword_helper.KeywordCategoryType(token_text, KeywordCategory::KEYWORD_UNRESERVED);
			const bool has_override_flag = keyword_helper.KeywordCategoryType(token_text, allowed_override_category);
			const bool has_allowed_flag = is_unreserved || has_override_flag;

			if (has_banned_flag && !has_allowed_flag) {
				return MatchResultType::FAIL;
			}
			break;
		}
		}
		if (!IsIdentifier(token_text)) {
			return MatchResultType::FAIL;
		}
		state.token_index++;
		return MatchResultType::SUCCESS;
	}

	bool SupportsStringLiteral() const {
		switch (suggestion_type) {
		case SuggestionState::SUGGEST_TABLE_NAME:
		case SuggestionState::SUGGEST_FILE_NAME:
			return true;
		default:
			return false;
		}
	}

	KeywordCategory GetBannedCategory() const {
		switch (suggestion_type) {
		case SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME:
		case SuggestionState::SUGGEST_TABLE_FUNCTION_NAME:
			return KeywordCategory::KEYWORD_COL_NAME;
		default:
			return KeywordCategory::KEYWORD_TYPE_FUNC;
		}
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		state.AddSuggestion(MatcherSuggestion(suggestion_type));
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		switch (suggestion_type) {
		case SuggestionState::SUGGEST_KEYWORD:
			return "KEYWORD";
		case SuggestionState::SUGGEST_CATALOG_NAME:
			return "CATALOG_NAME";
		case SuggestionState::SUGGEST_SCHEMA_NAME:
			return "SCHEMA_NAME";
		case SuggestionState::SUGGEST_TABLE_NAME:
			return "TABLE_NAME";
		case SuggestionState::SUGGEST_TYPE_NAME:
			return "TYPE_NAME";
		case SuggestionState::SUGGEST_COLUMN_NAME:
			return "COLUMN_NAME";
		case SuggestionState::SUGGEST_FILE_NAME:
			return "FILE_NAME";
		case SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME:
			return "SCALAR_FUNCTION_NAME";
		case SuggestionState::SUGGEST_TABLE_FUNCTION_NAME:
			return "TABLE_FUNCTION_NAME";
		case SuggestionState::SUGGEST_PRAGMA_NAME:
			return "PRAGMA_NAME";
		case SuggestionState::SUGGEST_SETTING_NAME:
			return "SETTING_NAME";
		case SuggestionState::SUGGEST_VARIABLE:
			return "VARIABLE";
		default:
			return "?VARIABLE?";
		}
	}

	SuggestionState suggestion_type;
};

class ReservedIdentifierMatcher : public IdentifierMatcher {
public:
	static constexpr MatcherType TYPE = MatcherType::VARIABLE;

public:
	explicit ReservedIdentifierMatcher(SuggestionState suggestion_type) : IdentifierMatcher(suggestion_type) {
	}

	MatchResultType Match(MatchState &state) const override {
		// reserved variable matchers match anything
		auto &token_text = state.tokens[state.token_index].text;
		if (!IsIdentifier(token_text)) {
			return MatchResultType::FAIL;
		}
		state.token_index++;
		return MatchResultType::SUCCESS;
	}
};

class StringLiteralMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::STRING_LITERAL;

public:
	explicit StringLiteralMatcher() : Matcher(TYPE) {
	}

	MatchResultType Match(MatchState &state) const override {
		// variable matchers match anything except for reserved keywords
		auto &token_text = state.tokens[state.token_index].text;
		if (token_text.size() >= 2 && token_text.front() == '\'' && token_text.back() == '\'') {
			state.token_index++;
			return MatchResultType::SUCCESS;
		}
		return MatchResultType::FAIL;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "STRING_LITERAL";
	}
};

class NumberLiteralMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::NUMBER_LITERAL;

public:
	explicit NumberLiteralMatcher() : Matcher(TYPE) {
	}

	MatchResultType Match(MatchState &state) const override {
		// variable matchers match anything except for reserved keywords
		auto &token_text = state.tokens[state.token_index].text;
		if (!BaseTokenizer::CharacterIsInitialNumber(token_text[0])) {
			return MatchResultType::FAIL;
		}
		for (idx_t i = 1; i < token_text.size(); i++) {
			if (!BaseTokenizer::CharacterIsNumber(token_text[i])) {
				return MatchResultType::FAIL;
			}
		}
		state.token_index++;
		return MatchResultType::SUCCESS;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "NUMBER_LITERAL";
	}
};

class OperatorMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::OPERATOR;

public:
	explicit OperatorMatcher() : Matcher(TYPE) {
	}

	MatchResultType Match(MatchState &state) const override {
		auto &token_text = state.tokens[state.token_index].text;
		for (auto &c : token_text) {
			switch (c) {
			case '+':
			case '-':
			case '*':
			case '/':
			case '%':
			case '^':
			case '<':
			case '>':
			case '=':
			case '~':
			case '!':
			case '@':
			case '&':
			case '|':
				break;
			default:
				return MatchResultType::FAIL;
			}
		}
		state.token_index++;
		return MatchResultType::SUCCESS;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "OPERATOR";
	}
};

Matcher &MatcherAllocator::Allocate(unique_ptr<Matcher> matcher) {
	auto &result = *matcher;
	matchers.push_back(std::move(matcher));
	return result;
}

//! Class for building matchers
class MatcherFactory {
	friend struct MatcherList;

public:
	explicit MatcherFactory(MatcherAllocator &allocator) : allocator(allocator) {
	}

	//! Create a matcher from a PEG grammar
	Matcher &CreateMatcher(const char *grammar, const char *root_rule);

private:
	// Base primitives
	Matcher &Keyword(const string &keyword) const;
	Matcher &List() const;
	Matcher &List(vector<reference<Matcher>> matchers) const;
	Matcher &Choice(vector<reference<Matcher>> matchers) const;
	Matcher &Optional(Matcher &matcher) const;
	Matcher &Repeat(Matcher &matcher) const;
	Matcher &Variable() const;
	Matcher &CatalogName() const;
	Matcher &SchemaName() const;
	Matcher &TypeName() const;
	Matcher &TableName() const;
	Matcher &ColumnName() const;
	Matcher &StringLiteral() const;
	Matcher &NumberLiteral() const;
	Matcher &Operator() const;
	Matcher &ScalarFunctionName() const;
	Matcher &TableFunctionName() const;
	Matcher &PragmaName() const;
	Matcher &SettingName() const;
	Matcher &ReservedSchemaName() const;
	Matcher &ReservedTableName() const;
	Matcher &ReservedColumnName() const;
	Matcher &ReservedScalarFunctionName() const;
	Matcher &ReservedVariable() const;

	void AddKeywordOverride(const char *name, uint32_t score, char extra_char = ' ');
	void AddRuleOverride(const char *name, Matcher &matcher);
	Matcher &CreateMatcher(PEGParser &parser, string_t rule_name);
	Matcher &CreateMatcher(PEGParser &parser, string_t rule_name, vector<reference<Matcher>> &parameters);

private:
	MatcherAllocator &allocator;
	string_map_t<reference<Matcher>> matchers;
	case_insensitive_map_t<reference<Matcher>> keyword_overrides;
};

Matcher &MatcherFactory::Keyword(const string &keyword) const {
	auto entry = keyword_overrides.find(keyword);
	if (entry != keyword_overrides.end()) {
		return entry->second.get();
	}
	return allocator.Allocate(make_uniq<KeywordMatcher>(keyword, 0, ' '));
}

Matcher &MatcherFactory::List() const {
	return allocator.Allocate(make_uniq<ListMatcher>());
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

Matcher &MatcherFactory::Repeat(Matcher &matcher) const {
	return allocator.Allocate(make_uniq<RepeatMatcher>(matcher));
}

Matcher &MatcherFactory::Variable() const {
	return allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE));
}

Matcher &MatcherFactory::ReservedVariable() const {
	return allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE));
}
Matcher &MatcherFactory::CatalogName() const {
	return allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_CATALOG_NAME));
}

Matcher &MatcherFactory::SchemaName() const {
	return allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_SCHEMA_NAME));
}

Matcher &MatcherFactory::ReservedSchemaName() const {
	return allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_SCHEMA_NAME));
}

Matcher &MatcherFactory::TableName() const {
	return allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_TABLE_NAME));
}

Matcher &MatcherFactory::ReservedTableName() const {
	return allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_TABLE_NAME));
}

Matcher &MatcherFactory::ColumnName() const {
	return allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_COLUMN_NAME));
}

Matcher &MatcherFactory::ReservedColumnName() const {
	return allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_COLUMN_NAME));
}

Matcher &MatcherFactory::TypeName() const {
	return allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_TYPE_NAME));
}

Matcher &MatcherFactory::ScalarFunctionName() const {
	return allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME));
}

Matcher &MatcherFactory::ReservedScalarFunctionName() const {
	return allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME));
}

Matcher &MatcherFactory::TableFunctionName() const {
	return allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_TABLE_FUNCTION_NAME));
}

Matcher &MatcherFactory::PragmaName() const {
	return allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_PRAGMA_NAME));
}

Matcher &MatcherFactory::SettingName() const {
	return allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_SETTING_NAME));
}

Matcher &MatcherFactory::NumberLiteral() const {
	return allocator.Allocate(make_uniq<NumberLiteralMatcher>());
}

Matcher &MatcherFactory::StringLiteral() const {
	return allocator.Allocate(make_uniq<StringLiteralMatcher>());
}

Matcher &MatcherFactory::Operator() const {
	return allocator.Allocate(make_uniq<OperatorMatcher>());
}

enum class PEGRuleType {
	LITERAL,   // literal rule ('Keyword'i)
	REFERENCE, // reference to another rule (Rule)
	OPTIONAL,  // optional rule (Rule?)
	OR,        // or rule (Rule1 / Rule2)
	REPEAT     // repeat rule (Rule1*
};

enum class PEGTokenType {
	LITERAL,       // literal token ('Keyword'i)
	REFERENCE,     // reference token (Rule)
	OPERATOR,      // operator token (/ or )
	FUNCTION_CALL, // start of function call (i.e. Function(...))
	REGEX          // regular expression ([ \t\n\r] or <[a-z_]i[a-z0-9_]i>)
};

struct PEGToken {
	PEGTokenType type;
	string_t text;
};

struct PEGRule {
	string_map_t<idx_t> parameters;
	vector<PEGToken> tokens;

	void Clear() {
		parameters.clear();
		tokens.clear();
	}
};

struct PEGParser {
public:
	void ParseRules(const char *grammar);

	void AddRule(string_t rule_name, PEGRule rule) {
		auto entry = rules.find(rule_name);
		if (entry != rules.end()) {
			throw InternalException("Failed to parse grammar - duplicate rule name %s", rule_name.GetString());
		}
		rules.insert(make_pair(rule_name, std::move(rule)));
	}

	string_map_t<PEGRule> rules;
};

enum class PEGParseState {
	RULE_NAME,      // Rule name
	RULE_SEPARATOR, // look for <-
	RULE_DEFINITION // part of rule definition
};

bool IsPEGOperator(char c) {
	switch (c) {
	case '/':
	case '?':
	case '(':
	case ')':
	case '*':
	case '!':
		return true;
	default:
		return false;
	}
}

void PEGParser::ParseRules(const char *grammar) {
	string_t rule_name;
	PEGRule rule;
	PEGParseState parse_state = PEGParseState::RULE_NAME;
	idx_t bracket_count = 0;
	bool in_or_clause = false;
	// look for the rules
	idx_t c = 0;
	while (grammar[c]) {
		if (grammar[c] == '#') {
			// comment - ignore until EOL
			while (grammar[c] && !StringUtil::CharacterIsNewline(grammar[c])) {
				c++;
			}
			continue;
		}
		if (parse_state == PEGParseState::RULE_DEFINITION && StringUtil::CharacterIsNewline(grammar[c]) &&
		    bracket_count == 0 && !in_or_clause && !rule.tokens.empty()) {
			// if we see a newline while we are parsing a rule definition we can complete the rule
			AddRule(rule_name, std::move(rule));
			rule_name = string_t();
			rule.Clear();
			// look for the subsequent rule
			parse_state = PEGParseState::RULE_NAME;
			c++;
			continue;
		}
		if (StringUtil::CharacterIsSpace(grammar[c])) {
			// skip whitespace
			c++;
			continue;
		}
		switch (parse_state) {
		case PEGParseState::RULE_NAME: {
			// look for alpha-numerics
			idx_t start_pos = c;
			if (grammar[c] == '%') {
				// rules can start with % (%whitespace)
				c++;
			}
			while (grammar[c] && StringUtil::CharacterIsAlphaNumeric(grammar[c])) {
				c++;
			}
			if (c == start_pos) {
				throw InternalException("Failed to parse grammar - expected an alpha-numeric rule name (pos %d)", c);
			}
			rule_name = string_t(grammar + start_pos, c - start_pos);
			rule.Clear();
			parse_state = PEGParseState::RULE_SEPARATOR;
			break;
		}
		case PEGParseState::RULE_SEPARATOR: {
			if (grammar[c] == '(') {
				if (!rule.parameters.empty()) {
					throw InternalException("Failed to parse grammar - multiple parameters at position %d", c);
				}
				// parameter
				c++;
				idx_t parameter_start = c;
				while (grammar[c] && StringUtil::CharacterIsAlphaNumeric(grammar[c])) {
					c++;
				}
				if (parameter_start == c) {
					throw InternalException("Failed to parse grammar - expected a parameter at position %d", c);
				}
				rule.parameters.insert(
				    make_pair(string_t(grammar + parameter_start, c - parameter_start), rule.parameters.size()));
				if (grammar[c] != ')') {
					throw InternalException("Failed to parse grammar - expected closing bracket at position %d", c);
				}
				c++;
			} else {
				if (grammar[c] != '<' || grammar[c + 1] != '-') {
					throw InternalException("Failed to parse grammar - expected a rule definition (<-) (pos %d)", c);
				}
				c += 2;
				parse_state = PEGParseState::RULE_DEFINITION;
			}
			break;
		}
		case PEGParseState::RULE_DEFINITION: {
			// we parse either:
			// (1) a literal ('Keyword'i)
			// (2) a rule reference (Rule)
			// (3) an operator ( '(' '/' '?' '*' ')')
			in_or_clause = false;
			if (grammar[c] == '\'') {
				// parse literal
				c++;
				idx_t literal_start = c;
				while (grammar[c] && grammar[c] != '\'') {
					if (grammar[c] == '\\') {
						// escape
						c++;
					}
					c++;
				}
				if (!grammar[c]) {
					throw InternalException("Failed to parse grammar - did not find closing ' (pos %d)", c);
				}
				PEGToken token;
				token.text = string_t(grammar + literal_start, c - literal_start);
				token.type = PEGTokenType::LITERAL;
				rule.tokens.push_back(token);
				c++;
			} else if (StringUtil::CharacterIsAlphaNumeric(grammar[c])) {
				// alphanumeric character - this is a rule reference
				idx_t rule_start = c;
				while (grammar[c] && StringUtil::CharacterIsAlphaNumeric(grammar[c])) {
					c++;
				}
				PEGToken token;
				token.text = string_t(grammar + rule_start, c - rule_start);
				if (grammar[c] == '(') {
					// this is a function call
					c++;
					bracket_count++;
					token.type = PEGTokenType::FUNCTION_CALL;
				} else {
					token.type = PEGTokenType::REFERENCE;
				}
				rule.tokens.push_back(token);
			} else if (grammar[c] == '[' || grammar[c] == '<') {
				// regular expression- [^"] or <...>
				idx_t rule_start = c;
				char final_char = grammar[c] == '[' ? ']' : '>';
				while (grammar[c] && grammar[c] != final_char) {
					if (grammar[c] == '\\') {
						// handle escapes
						c++;
					}
					if (grammar[c]) {
						c++;
					}
				}
				c++;
				PEGToken token;
				token.text = string_t(grammar + rule_start, c - rule_start);
				token.type = PEGTokenType::REGEX;
				rule.tokens.push_back(token);
			} else if (IsPEGOperator(grammar[c])) {
				if (grammar[c] == '(') {
					bracket_count++;
				} else if (grammar[c] == ')') {
					if (bracket_count == 0) {
						throw InternalException("Failed to parse grammar - unclosed bracket at position %d in rule %s",
						                        c, rule_name.GetString());
					}
					bracket_count--;
				} else if (grammar[c] == '/') {
					in_or_clause = true;
				}
				// operator - operators are always length 1
				PEGToken token;
				token.text = string_t(grammar + c, 1);
				token.type = PEGTokenType::OPERATOR;
				rule.tokens.push_back(token);
				c++;
			} else {
				throw InternalException("Unrecognized rule contents in rule %s (character %s)", rule_name.GetString(),
				                        string(1, grammar[c]));
			}
		}
		default:
			break;
		}
		if (!grammar[c]) {
			break;
		}
	}
	if (parse_state == PEGParseState::RULE_SEPARATOR) {
		throw InternalException("Failed to parse grammar - rule %s does not have a definition", rule_name.GetString());
	}
	if (parse_state == PEGParseState::RULE_DEFINITION) {
		if (rule.tokens.empty()) {
			throw InternalException("Failed to parse grammar - rule %s is empty", rule_name.GetString());
		}
		AddRule(rule_name, std::move(rule));
	}
}

Matcher &MatcherFactory::CreateMatcher(PEGParser &parser, string_t rule_name) {
	vector<reference<Matcher>> parameters;
	return CreateMatcher(parser, rule_name, parameters);
}

struct MatcherListEntry {
	explicit MatcherListEntry(Matcher &matcher) : matcher(matcher), function_name(0U) {
	}
	MatcherListEntry(Matcher &matcher, string_t function_name_p) : matcher(matcher), function_name(function_name_p) {
	}

	Matcher &matcher;
	string_t function_name;
};

struct MatcherList {
public:
	explicit MatcherList(PEGParser &parser, MatcherFactory &factory) : parser(parser), factory(factory) {
	}

	void AddMatcher(Matcher &matcher) {
		auto &root_matcher = matchers.back().matcher;
		switch (root_matcher.Type()) {
		case MatcherType::LIST: {
			auto &root_list = root_matcher.Cast<ListMatcher>();
			root_list.matchers.push_back(matcher);
			break;
		}
		case MatcherType::CHOICE:
			// for a choice matcher we need to pop the choice matcher from the stack afterwards
			if (matchers.size() <= 1) {
				throw InternalException("Choice matcher should never be the root in the matcher stack");
			}
			root_matcher.Cast<ChoiceMatcher>().matchers.push_back(matcher);
			matchers.pop_back();
			break;
		default:
			throw InternalException("Cannot add matcher to root matcher of this type");
		}
	}
	void AddRootMatcher(Matcher &matcher) {
		matchers.emplace_back(matcher);
	}
	idx_t GetRootMatcherCount() const {
		return matchers.size();
	}
	MatcherListEntry &GetLastRootMatcher() {
		return matchers.back();
	}
	void BeginFunction(string_t function_name) {
		auto &parameter_list = factory.List();
		matchers.emplace_back(parameter_list, function_name);
	}
	void CloseBracket() {
		if (matchers.size() <= 1) {
			throw InternalException("PEG matcher create error - found too many close brackets");
		}
		auto &root_bracket_matcher = matchers.back();
		if (root_bracket_matcher.function_name.GetSize() == 0) {
			// not a function
			auto &bracket_matcher = root_bracket_matcher.matcher;
			// remove the last matcher from the stack
			matchers.pop_back();
			// push it into the last matcher
			AddMatcher(bracket_matcher);
		} else {
			// function matcher
			auto &function_name = root_bracket_matcher.function_name;
			auto &function_parameters = root_bracket_matcher.matcher.Cast<ListMatcher>();

			// wrap the parameters in a list if there is more than one
			auto &parameter = function_parameters.matchers.size() == 1 ? function_parameters.matchers[0].get()
			                                                           : factory.List(function_parameters.matchers);
			vector<reference<Matcher>> parameters;
			parameters.push_back(parameter);
			// do the substitution of the function call
			auto &function_call = factory.CreateMatcher(parser, function_name, parameters);
			// remove the last matcher from the stack
			matchers.pop_back();
			// push it into the last matcher
			AddMatcher(function_call);
		}
	}

private:
	PEGParser &parser;
	MatcherFactory &factory;
	vector<MatcherListEntry> matchers;
};

Matcher &MatcherFactory::CreateMatcher(PEGParser &parser, string_t rule_name, vector<reference<Matcher>> &parameters) {
	bool is_function_call = !parameters.empty();
	if (!is_function_call) {
		// check if the matcher has already been created first
		auto matcher_entry = matchers.find(rule_name);
		if (matcher_entry != matchers.end()) {
			// return the created matcher
			return matcher_entry->second.get();
		}
	}
	// look up the rule
	auto entry = parser.rules.find(rule_name);
	if (entry == parser.rules.end()) {
		throw InternalException("Failed to create matcher for rule %s - rule is missing", rule_name.GetString());
	}
	// create a matcher and cache it
	// since matchers can be recursive we need to cache it prior to recursively constructing the other rules
	auto &matcher = List();
	if (!is_function_call) {
		matchers.insert(make_pair(rule_name, reference<Matcher>(matcher)));
	}

	MatcherList list(parser, *this);
	list.AddRootMatcher(matcher);
	// fill the matcher from the given set of rules
	auto &rule = entry->second;
	if (rule.parameters.size() > 1) {
		throw InternalException("Only functions with a single parameter are supported");
	}
	if (parameters.size() != rule.parameters.size()) {
		throw InternalException("Parameter count mismatch (rule %s expected %d parameters but got %d)",
		                        rule_name.GetString(), rule.parameters.size(), parameters.size());
	}
	for (idx_t token_idx = 0; token_idx < rule.tokens.size(); token_idx++) {
		auto &token = rule.tokens[token_idx];
		switch (token.type) {
		case PEGTokenType::LITERAL:
			// literal - push the keyword
			list.AddMatcher(Keyword(token.text.GetString()));
			break;
		case PEGTokenType::REFERENCE: {
			// check if we are referring to a keyword
			auto param_entry = rule.parameters.find(token.text);
			if (param_entry != rule.parameters.end()) {
				// refers to a parameter - refer to it directly
				list.AddMatcher(parameters[param_entry->second].get());
			} else {
				// refers to a different rule - create the matcher for that rule
				list.AddMatcher(CreateMatcher(parser, token.text));
			}
			break;
		}
		case PEGTokenType::FUNCTION_CALL: {
			// function call - get the name of the function
			list.BeginFunction(token.text);
			break;
		}
		case PEGTokenType::OPERATOR: {
			// tokens need to be one byte
			auto op_type = token.text.GetData()[0];
			switch (op_type) {
			case '?':
			case '*': {
				// optional/repeat - make the last rule optional/repeat
				auto &last_matcher = list.GetLastRootMatcher().matcher;
				if (last_matcher.Type() != MatcherType::LIST) {
					throw InternalException("Optional/Repeat expected a list matcher");
				}
				auto &list_matcher = last_matcher.Cast<ListMatcher>();
				if (list_matcher.matchers.empty()) {
					throw InternalException("Optional/Repeat rule found as first token");
				}
				auto &final_matcher = list_matcher.matchers.back();
				if (op_type == '*') {
					// * is Optional(Repeat(CHILD))
					final_matcher = Repeat(final_matcher.get());
				}
				auto &replaced_matcher = Optional(final_matcher);
				list_matcher.matchers.pop_back();
				list_matcher.matchers.push_back(replaced_matcher);
				break;
			}
			case '/': {
				// OR operator - this signifies a choice between the last rule and the next rule
				auto &last_matcher = list.GetLastRootMatcher().matcher;
				if (last_matcher.Type() != MatcherType::LIST) {
					throw InternalException("OR expected a list matcher");
				}
				auto &list_matcher = last_matcher.Cast<ListMatcher>();
				if (list_matcher.matchers.empty()) {
					throw InternalException("OR rule found as first token");
				}
				auto &final_matcher = list_matcher.matchers.back();
				vector<reference<Matcher>> choice_matchers;
				choice_matchers.push_back(final_matcher);
				auto &choice_matcher = Choice(choice_matchers);

				// the choice matcher gets added to the list matcher (instead of the previous matcher)
				list_matcher.matchers.pop_back();
				list_matcher.matchers.push_back(choice_matcher);
				// then it gets pushed onto the stack of matchers
				// the next rule will then get pushed onto the choice matcher
				list.AddRootMatcher(choice_matcher);
				break;
			}
			case '(': {
				// bracket open - push a new list matcher onto the stack
				auto &bracket_matcher = List();
				list.AddRootMatcher(bracket_matcher);
				break;
			}
			case ')': {
				list.CloseBracket();
				break;
			}
			case '!': {
				// throw InternalException("NOT operator not supported in PEG grammar (found in rule %s)",
				// rule_name.GetString());
				// FIXME: we just ignore NOT operators here
				break;
			}
			default:
				throw InternalException("unrecognized peg operator type");
			}
			break;
		}
		case PEGTokenType::REGEX:
			throw InternalException("REGEX operator not supported in PEG grammar (found in rule %s)",
			                        rule_name.GetString());
		default:
			throw InternalException("unrecognized peg token type");
		}
	}
	if (list.GetRootMatcherCount() != 1) {
		throw InternalException("PEG matcher create error - unclosed bracket found");
	}
	matcher.SetName(rule_name.GetString());
	return matcher;
}

void MatcherFactory::AddKeywordOverride(const char *name, uint32_t score, char extra_char) {
	auto &keyword_matcher = allocator.Allocate(make_uniq<KeywordMatcher>(name, score, extra_char));
	keyword_overrides.insert(make_pair(name, reference<Matcher>(keyword_matcher)));
}

void MatcherFactory::AddRuleOverride(const char *name, Matcher &matcher) {
	matchers.insert(make_pair(name, reference<Matcher>(matcher)));
}

Matcher &MatcherFactory::CreateMatcher(const char *grammar, const char *root_rule) {
	// parse the grammar into a set of rules
	PEGParser parser;
	parser.ParseRules(grammar);

	// keyword overrides
	AddKeywordOverride("TABLE", 1, ' ');
	AddKeywordOverride(".", 0, '\0');
	AddKeywordOverride("(", 0, '\0');
	// rule overrides
	AddRuleOverride("Identifier", Variable());
	AddRuleOverride("ReservedIdentifier", ReservedVariable());
	AddRuleOverride("TypeName", TypeName());
	AddRuleOverride("TableName", TableName());
	AddRuleOverride("ReservedTableName", ReservedTableName());
	AddRuleOverride("CatalogName", CatalogName());
	AddRuleOverride("SchemaName", SchemaName());
	AddRuleOverride("ReservedSchemaName", ReservedSchemaName());
	AddRuleOverride("ColumnName", ColumnName());
	AddRuleOverride("ReservedColumnName", ReservedColumnName());
	AddRuleOverride("TableFunctionName", TableFunctionName());
	AddRuleOverride("FunctionName", ScalarFunctionName());
	AddRuleOverride("ReservedFunctionName", ReservedScalarFunctionName());
	AddRuleOverride("PragmaName", PragmaName());
	AddRuleOverride("SettingName", SettingName());
	AddRuleOverride("NumberLiteral", NumberLiteral());
	AddRuleOverride("StringLiteral", StringLiteral());
	AddRuleOverride("OperatorLiteral", Operator());

	// now create the matchers for each of the rules recursively - starting at the root rule
	return CreateMatcher(parser, root_rule);
}

Matcher &Matcher::RootMatcher(MatcherAllocator &allocator) {
	MatcherFactory factory(allocator);
#ifdef PEG_PARSER_SOURCE_FILE
	std::ifstream t(PEG_PARSER_SOURCE_FILE);
	std::stringstream buffer;
	buffer << t.rdbuf();
	auto string = buffer.str();

	return factory.CreateMatcher(string.c_str(), "Statement");
#else
	return factory.CreateMatcher(const_char_ptr_cast(INLINED_PEG_GRAMMAR), "Statement");
#endif
}

} // namespace duckdb
