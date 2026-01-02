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
#include "parser/peg_parser.hpp"
#include "transformer/parse_result.hpp"
#ifdef PEG_PARSER_SOURCE_FILE
#include <fstream>
#else
#include "inlined_grammar.hpp"
#endif

namespace duckdb {

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
		if (!MatchKeyword(state)) {
			return MatchResultType::FAIL;
		}
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResult(MatchState &state) const override {
		if (state.token_index >= state.tokens.size()) {
			return nullptr;
		}
		auto &token_text = state.tokens[state.token_index].text;
		if (!MatchKeyword(state)) {
			return nullptr;
		}
		auto result = state.allocator.Allocate(make_uniq<KeywordParseResult>(token_text));
		result->name = name;
		return result;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		AutoCompleteCandidate candidate(keyword, SuggestionState::SUGGEST_KEYWORD, score_bonus, CandidateType::KEYWORD);
		candidate.extra_char = extra_char;
		state.AddSuggestion(MatcherSuggestion(std::move(candidate)));
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "'" + keyword + "'";
	}

private:
	bool MatchKeyword(MatchState &state) const {
		if (state.token_index >= state.tokens.size()) {
			return false;
		}
		auto &token = state.tokens[state.token_index];
		if (StringUtil::CIEquals(keyword, token.text)) {
			// move to the next token
			state.token_index++;
			return true;
		}
		return false;
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

	optional_ptr<ParseResult> MatchParseResult(MatchState &state) const override {
		MatchState list_state(state);
		vector<optional_ptr<ParseResult>> results;

		for (const auto &child_matcher : matchers) {
			auto child_result = child_matcher.get().MatchParseResult(list_state);
			if (!child_result) {
				return nullptr;
			}
			results.push_back(child_result);
		}
		state.token_index = list_state.token_index;
		// Empty name implies it's a subrule, e.g. 'SET'i (StandardAssignment / SetTimeZone)
		return state.allocator.Allocate(make_uniq<ListParseResult>(std::move(results), name));
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
		if (child_match == MatchResultType::FAIL) {
			// did not succeed in matching - go back up (but return success anyway)
			return MatchResultType::SUCCESS;
		}
		// propagate the child state upwards
		state.token_index = child_state.token_index;
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResult(MatchState &state) const override {
		MatchState child_state(state);
		auto child_match = matcher.MatchParseResult(child_state);
		if (child_match == nullptr) {
			// did not succeed in matching - go back up (simply return a nullptr)
			return state.allocator.Allocate(make_uniq<OptionalParseResult>());
		}
		// propagate the child state upwards
		state.token_index = child_state.token_index;
		return state.allocator.Allocate(make_uniq<OptionalParseResult>(child_match));
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

	optional_ptr<ParseResult> MatchParseResult(MatchState &state) const override {
		for (idx_t i = 0; i < matchers.size(); i++) {
			MatchState choice_state(state);
			auto child_result = matchers[i].get().MatchParseResult(choice_state);
			if (child_result != nullptr) {
				// we matched this child - propagate upwards
				state.token_index = choice_state.token_index;
				auto result = state.allocator.Allocate(make_uniq<ChoiceParseResult>(child_result, i));
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

	optional_ptr<ParseResult> MatchParseResult(MatchState &state) const override {
		MatchState repeat_state(state);
		vector<optional_ptr<ParseResult>> results;

		// First, we MUST match the element at least once.
		auto first_result = element.MatchParseResult(repeat_state);
		if (!first_result) {
			// The first match failed, so the whole repeat fails.
			return nullptr;
		}
		results.push_back(first_result);

		// After the first success, the overall result is a success.
		// Now, we continue matching the element as many times as possible.
		while (true) {
			// Propagate the new state upwards.
			state.token_index = repeat_state.token_index;

			// Check if there are any tokens left.
			if (repeat_state.token_index >= state.tokens.size()) {
				break;
			}

			// Try to match the element again.
			auto next_result = element.MatchParseResult(repeat_state);
			if (!next_result) {
				break;
			}
			results.push_back(next_result);
		}

		// Return all collected results in a RepeatParseResult.
		return state.allocator.Allocate(make_uniq<RepeatParseResult>(std::move(results)));
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

	bool IsQuoted(const string &text) const {
		if (text.front() == '"' && text.back() == '"') {
			return true;
		}
		return false;
	}

	bool IsIdentifier(const string &text) const {
		if (text.empty()) {
			return false;
		}
		if (text.front() == '\'' && text.back() == '\'' && SupportsStringLiteral()) {
			return true;
		}
		if (IsQuoted(text)) {
			return true;
		}
		return BaseTokenizer::CharacterIsKeyword(text[0]);
	}

	MatchResultType Match(MatchState &state) const override {
		if (!MatchIdentifier(state)) {
			return MatchResultType::FAIL;
		}
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResult(MatchState &state) const override {
		if (state.token_index >= state.tokens.size()) {
			return nullptr;
		}
		auto &token_text = state.tokens[state.token_index].text;
		if (!MatchIdentifier(state)) {
			return nullptr;
		}
		if (IsQuoted(token_text)) {
			token_text = token_text.substr(1, token_text.size() - 2);
		}
		return state.allocator.Allocate(make_uniq<IdentifierParseResult>(token_text));
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

	PEGKeywordCategory GetBannedCategory() const {
		switch (suggestion_type) {
		case SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME:
		case SuggestionState::SUGGEST_TABLE_FUNCTION_NAME:
			return PEGKeywordCategory::KEYWORD_COL_NAME;
		default:
			return PEGKeywordCategory::KEYWORD_TYPE_FUNC;
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

private:
	bool MatchIdentifier(MatchState &state) const {
		// variable matchers match anything except for reserved keywords
		auto &token_text = state.tokens[state.token_index].text;
		const auto &keyword_helper = PEGKeywordHelper::Instance();
		switch (suggestion_type) {
		case SuggestionState::SUGGEST_TYPE_NAME:
			if (keyword_helper.KeywordCategoryType(token_text, PEGKeywordCategory::KEYWORD_RESERVED) ||
			    keyword_helper.KeywordCategoryType(token_text, GetBannedCategory())) {
				return false;
			}
			break;
		default: {
			const auto banned_category = GetBannedCategory();
			const auto allowed_override_category = banned_category == PEGKeywordCategory::KEYWORD_COL_NAME
			                                           ? PEGKeywordCategory::KEYWORD_TYPE_FUNC
			                                           : PEGKeywordCategory::KEYWORD_COL_NAME;

			const bool is_reserved =
			    keyword_helper.KeywordCategoryType(token_text, PEGKeywordCategory::KEYWORD_RESERVED);
			const bool has_extra_banned_category = keyword_helper.KeywordCategoryType(token_text, banned_category);
			const bool has_banned_flag = is_reserved || has_extra_banned_category;

			const bool is_unreserved =
			    keyword_helper.KeywordCategoryType(token_text, PEGKeywordCategory::KEYWORD_UNRESERVED);
			const bool has_override_flag = keyword_helper.KeywordCategoryType(token_text, allowed_override_category);
			const bool has_allowed_flag = is_unreserved || has_override_flag;

			if (has_banned_flag && !has_allowed_flag) {
				return false;
			}
			break;
		}
		}
		if (!IsIdentifier(token_text)) {
			return false;
		}
		state.token_index++;
		return true;
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
		if (!MatchReservedIdentifier(state)) {
			return MatchResultType::FAIL;
		}
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResult(MatchState &state) const override {
		auto &token_text = state.tokens[state.token_index].text;
		if (!MatchReservedIdentifier(state)) {
			return nullptr;
		}
		if (IsQuoted(token_text)) {
			token_text = token_text.substr(1, token_text.size() - 2);
		}
		return state.allocator.Allocate(make_uniq<IdentifierParseResult>(token_text));
	}

private:
	bool MatchReservedIdentifier(MatchState &state) const {
		auto &token_text = state.tokens[state.token_index].text;
		if (!IsIdentifier(token_text)) {
			return false;
		}
		state.token_index++;
		return true;
	}
};

class StringLiteralMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::STRING_LITERAL;

public:
	explicit StringLiteralMatcher() : Matcher(TYPE) {
		name = "StringLiteral";
	}

	MatchResultType Match(MatchState &state) const override {
		// variable matchers match anything except for reserved keywords
		if (!MatchStringLiteral(state)) {
			return MatchResultType::FAIL;
		}
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResult(MatchState &state) const override {
		if (state.token_index >= state.tokens.size()) {
			return nullptr;
		}
		auto &token_text = state.tokens[state.token_index].text;
		if (!MatchStringLiteral(state)) {
			return nullptr;
		}
		string stripped_string = token_text.substr(1, token_text.length() - 2);

		auto result = state.allocator.Allocate(make_uniq<StringLiteralParseResult>(stripped_string));
		result->name = name;
		return result;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "STRING_LITERAL";
	}

private:
	static bool MatchStringLiteral(MatchState &state) {
		auto &token_text = state.tokens[state.token_index].text;
		if (token_text.size() >= 2 && token_text.front() == '\'' && token_text.back() == '\'') {
			state.token_index++;
			return true;
		}
		return false;
	}
};

class NumberLiteralMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::NUMBER_LITERAL;

public:
	explicit NumberLiteralMatcher() : Matcher(TYPE) {
		name = "NumberLiteral";
	}

	MatchResultType Match(MatchState &state) const override {
		// variable matchers match anything except for reserved keywords
		if (!MatchNumberLiteral(state)) {
			return MatchResultType::FAIL;
		}
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResult(MatchState &state) const override {
		if (state.token_index >= state.tokens.size()) {
			return nullptr;
		}
		auto &token_text = state.tokens[state.token_index].text;
		if (!MatchNumberLiteral(state)) {
			return nullptr;
		}
		auto result = state.allocator.Allocate(make_uniq<NumberParseResult>(token_text));
		result->name = name;
		return result;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "NUMBER_LITERAL";
	}

private:
	static bool MatchNumberLiteral(MatchState &state) {
		auto &token_text = state.tokens[state.token_index].text;
		if (!BaseTokenizer::CharacterIsInitialNumber(token_text[0])) {
			return false;
		}
		bool scientific_notation = false;
		for (idx_t i = 1; i < token_text.size(); i++) {
			if (BaseTokenizer::CharacterIsScientific(token_text[i])) {
				if (scientific_notation) {
					throw ParserException("Already found scientific notation");
				}
				scientific_notation = true;
			}
			if (scientific_notation && (token_text[i] == '+' || token_text[i] == '-')) {
				continue;
			}
			if (!BaseTokenizer::CharacterIsNumber(token_text[i])) {
				return false;
			}
		}
		state.token_index++;
		return true;
	}
};

class OperatorMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::OPERATOR;

public:
	explicit OperatorMatcher() : Matcher(TYPE) {
	}

	MatchResultType Match(MatchState &state) const override {
		if (!MatchOperator(state)) {
			return MatchResultType::FAIL;
		}
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResult(MatchState &state) const override {
		if (state.token_index >= state.tokens.size()) {
			return nullptr;
		}
		auto &token_text = state.tokens[state.token_index].text;
		if (!MatchOperator(state)) {
			return nullptr;
		}
		return state.allocator.Allocate(make_uniq<OperatorParseResult>(token_text));
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "OPERATOR";
	}

private:
	static bool MatchOperator(MatchState &state) {
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
				return false;
			}
		}
		state.token_index++;
		return true;
	}
};

class ArithmeticOperatorMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::OPERATOR;

public:
	explicit ArithmeticOperatorMatcher() : Matcher(TYPE) {
	}

	MatchResultType Match(MatchState &state) const override {
		if (!MatchArithmeticOperator(state)) {
			return MatchResultType::FAIL;
		}
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResult(MatchState &state) const override {
		if (state.token_index >= state.tokens.size()) {
			return nullptr;
		}
		auto &token_text = state.tokens[state.token_index].text;
		if (!MatchArithmeticOperator(state)) {
			return nullptr;
		}
		Printer::Print("Found arithmetic operator!!!");
		return state.allocator.Allocate(make_uniq<OperatorParseResult>(token_text));
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "ARITHMETICOPERATOR";
	}

private:
	static bool MatchArithmeticOperator(MatchState &state) {
		auto &token_text = state.tokens[state.token_index].text;
		for (auto &c : token_text) {
			switch (c) {
			case '*':
			case '/':
			case '+':
			case '-':
			case '%':
				break;
			default:
				return false;
			}
		}
		state.token_index++;
		return true;
	}
};

Matcher &MatcherAllocator::Allocate(unique_ptr<Matcher> matcher) {
	auto &result = *matcher;
	matchers.push_back(std::move(matcher));
	return result;
}

optional_ptr<ParseResult> ParseResultAllocator::Allocate(unique_ptr<ParseResult> parse_result) {
	auto result_ptr = parse_result.get();
	parse_results.push_back(std::move(parse_result));
	return optional_ptr<ParseResult>(result_ptr);
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
	Matcher &ArithmeticOperator() const;
	Matcher &ScalarFunctionName() const;
	Matcher &TableFunctionName() const;
	Matcher &PragmaName() const;
	Matcher &SettingName() const;
	Matcher &CopyOptionName() const;
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

Matcher &MatcherFactory::CopyOptionName() const {
	return allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE));
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

Matcher &MatcherFactory::ArithmeticOperator() const {
	return allocator.Allocate(make_uniq<ArithmeticOperatorMatcher>());
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
	auto entry = parser.rules.find(rule_name.GetString());
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
			case '+': {
				// Similar to '*' except it's not optional and just repeat (match at least once)
				auto &last_matcher = list.GetLastRootMatcher().matcher;
				if (last_matcher.Type() != MatcherType::LIST) {
					throw InternalException("Repeat expected a list matcher");
				}
				auto &list_matcher = last_matcher.Cast<ListMatcher>();
				if (list_matcher.matchers.empty()) {
					throw InternalException("Repeat rule found as first token");
				}
				auto &final_matcher = list_matcher.matchers.back();
				final_matcher = Repeat(final_matcher.get());
				list_matcher.matchers.pop_back();
				list_matcher.matchers.push_back(final_matcher);
				break;
			}
			case '/': {
				// OR operator - this signifies a choice between the last rule and the next rule
				auto &last_root_matcher = list.GetLastRootMatcher().matcher;
				if (last_root_matcher.Type() != MatcherType::LIST) {
					throw InternalException("OR expected a list matcher");
				}
				auto &list_matcher = last_root_matcher.Cast<ListMatcher>();
				if (list_matcher.matchers.empty()) {
					throw InternalException("OR rule found as first token");
				}
				auto &previous_matcher = list_matcher.matchers.back();

				if (previous_matcher.get().Type() == MatcherType::CHOICE) {
					list.AddRootMatcher(previous_matcher);
				} else {
					vector<reference<Matcher>> choice_options;
					choice_options.push_back(previous_matcher);
					auto &new_choice_matcher = Choice(choice_options);

					list_matcher.matchers.pop_back();
					list_matcher.matchers.push_back(new_choice_matcher);

					list.AddRootMatcher(new_choice_matcher);
				}
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

	AddRuleOverride("CatalogName", CatalogName());
	AddRuleOverride("SchemaName", SchemaName());
	AddRuleOverride("ReservedSchemaName", ReservedSchemaName());
	AddRuleOverride("TableName", TableName());
	AddRuleOverride("ReservedTableName", ReservedTableName());
	AddRuleOverride("ColumnName", ColumnName());
	AddRuleOverride("ReservedColumnName", ReservedColumnName());
	AddRuleOverride("IndexName", Variable());
	AddRuleOverride("SequenceName", Variable());

	AddRuleOverride("FunctionName", ScalarFunctionName());
	AddRuleOverride("ReservedFunctionName", ReservedScalarFunctionName());
	AddRuleOverride("TableFunctionName", TableFunctionName());

	AddRuleOverride("TypeName", TypeName());
	AddRuleOverride("PragmaName", PragmaName());
	AddRuleOverride("SettingName", SettingName());
	AddRuleOverride("CopyOptionName", CopyOptionName());

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
