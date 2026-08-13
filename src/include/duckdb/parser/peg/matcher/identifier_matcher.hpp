#pragma once

#include "duckdb/parser/peg/tokenizer/base_tokenizer.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

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

	bool IsSingleQuoted(const string &text) const {
		if (text.front() == '\'' && text.back() == '\'') {
			return true;
		}
		return false;
	}

	bool IsIdentifier(const string &text) const {
		if (text.empty()) {
			return false;
		}
		if (IsSingleQuoted(text) && SupportsStringLiteral()) {
			return true;
		}
		if (IsQuoted(text)) {
			return true;
		}
		if (BaseTokenizer::CharacterIsInitialNumber(text[0])) {
			return false;
		}
		return BaseTokenizer::CharacterIsKeyword(text[0]);
	}

	MatchResultType Match(MatchState &state) const override {
		if (!MatchIdentifier(state)) {
			return MatchResultType::FAIL;
		}
		state.tokens[state.token_index - 1].type = GetTokenType();
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		if (state.token_index >= state.tokens.size()) {
			return nullptr;
		}
		const auto &token_text = state.tokens[state.token_index].text;
		auto start_offset = optional_idx(state.tokens[state.token_index].offset);
		auto token_length = optional_idx(state.tokens[state.token_index].length);
		if (!MatchIdentifier(state)) {
			return nullptr;
		}

		string result_text = token_text;
		if (IsQuoted(result_text)) {
			result_text = result_text.substr(1, result_text.size() - 2);
			result_text = StringUtil::Replace(result_text, "\"\"", "\"");
		} else if (!state.preserve_identifier_case) {
			result_text = StringUtil::Lower(result_text);
		}
		if (IsSingleQuoted(result_text) && SupportsStringLiteral()) {
			result_text = result_text.substr(1, result_text.size() - 2);
			result_text = StringUtil::Replace(result_text, "''", "'");
		}
		return state.allocator.Allocate(make_uniq<IdentifierParseResult>(result_text, start_offset, token_length));
	}

	TokenType GetTokenType() const {
		switch (suggestion_type) {
		case SuggestionState::SUGGEST_CATALOG_NAME:
			return TokenType::CATALOG_NAME;
		case SuggestionState::SUGGEST_SCHEMA_NAME:
			return TokenType::SCHEMA_NAME;
		case SuggestionState::SUGGEST_TABLE_NAME:
			return TokenType::TABLE_NAME;
		case SuggestionState::SUGGEST_TYPE_NAME:
			return TokenType::TYPE_NAME;
		case SuggestionState::SUGGEST_COLUMN_NAME:
			return TokenType::COLUMN_NAME;
		case SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME:
			return TokenType::SCALAR_FUNCTION;
		case SuggestionState::SUGGEST_TABLE_FUNCTION_NAME:
			return TokenType::TABLE_FUNCTION;
		case SuggestionState::SUGGEST_PRAGMA_NAME:
			return TokenType::PRAGMA_FUNCTION;
		case SuggestionState::SUGGEST_SETTING_NAME:
			return TokenType::SETTING_NAME;
		default:
			return TokenType::IDENTIFIER;
		}
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
		if (state.token_index >= state.tokens.size()) {
			return false;
		}
		// variable matchers match anything except for reserved keywords
		auto &token_text = state.tokens[state.token_index].text;
		const auto &keyword_helper = PEGKeywordHelper::Instance();
		switch (suggestion_type) {
		case SuggestionState::SUGGEST_TYPE_NAME:
			if (keyword_helper.KeywordCategoryType(token_text, PEGKeywordCategory::KEYWORD_UNRESERVED) ||
			    keyword_helper.KeywordCategoryType(token_text, PEGKeywordCategory::KEYWORD_TYPE_NAME)) {
				break;
			}
			if (keyword_helper.KeywordCategoryType(token_text, PEGKeywordCategory::KEYWORD_RESERVED) ||
			    keyword_helper.KeywordCategoryType(token_text, PEGKeywordCategory::KEYWORD_TYPE_FUNC) ||
			    keyword_helper.KeywordCategoryType(token_text, PEGKeywordCategory::KEYWORD_COL_NAME)) {
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
		state.UpdateMaxTokenIndex();
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
		state.tokens[state.token_index - 1].type = GetTokenType();
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		if (state.token_index >= state.tokens.size()) {
			return nullptr;
		}
		auto &token_text = state.tokens[state.token_index].text;
		auto start_offset = optional_idx(state.tokens[state.token_index].offset);
		auto token_length = optional_idx(state.tokens[state.token_index].length);
		if (!MatchReservedIdentifier(state)) {
			return nullptr;
		}
		string result_text = token_text;
		if (IsQuoted(result_text)) {
			result_text = result_text.substr(1, result_text.size() - 2);
			result_text = StringUtil::Replace(result_text, "\"\"", "\"");
		} else if (!state.preserve_identifier_case) {
			result_text = StringUtil::Lower(result_text);
		}
		return state.allocator.Allocate(make_uniq<IdentifierParseResult>(result_text, start_offset, token_length));
	}

private:
	bool MatchReservedIdentifier(MatchState &state) const {
		if (state.token_index >= state.tokens.size()) {
			return false;
		}
		auto &token_text = state.tokens[state.token_index].text;
		if (!IsIdentifier(token_text)) {
			return false;
		}
		state.token_index++;
		state.UpdateMaxTokenIndex();
		return true;
	}
};

} // namespace duckdb
