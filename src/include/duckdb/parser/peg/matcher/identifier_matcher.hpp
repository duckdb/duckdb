#pragma once

#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

class IdentifierMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::VARIABLE;

public:
	IdentifierMatcher(SuggestionState suggestion_type, const PEGKeywordHelper &keyword_helper_p)
	    : Matcher(TYPE), suggestion_type(suggestion_type), keyword_helper(keyword_helper_p) {
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
		if (Tokenizer::CharacterIsInitialNumber(text[0])) {
			return false;
		}
		return Tokenizer::CharacterIsKeyword(text[0]);
	}

	MatcherResult MatchParseResultInternal(MatchState &state) const override {
		auto token = state.token_iterator.Current();
		if (!token) {
			return MatcherResult::Failure();
		}
		const auto &token_text = token->text;
		auto start_offset = optional_idx(token->offset);
		auto token_length = optional_idx(token->length);
		if (!MatchIdentifier(state)) {
			return MatcherResult::Failure();
		}
		state.token_iterator.SetPreviousTokenType(GetTokenType());
		if (!state.BuildParseResult()) {
			return MatcherResult::Success();
		}

		string result_text = token_text;
		if (IsQuoted(result_text)) {
			result_text = result_text.substr(1, result_text.size() - 2);
			result_text = StringUtil::Replace(result_text, "\"\"", "\"");
		} else if (IsSingleQuoted(result_text) && SupportsStringLiteral()) {
			// a single-quoted token in a table or file-name position is a path, so it is unwrapped but never folded
			result_text = result_text.substr(1, result_text.size() - 2);
			result_text = StringUtil::Replace(result_text, "''", "'");
		} else {
			state.FoldIdentifier(result_text);
		}
		return state.AllocateParseResult<IdentifierParseResult>(result_text, start_offset, token_length);
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

	PEGKeywordCategory GetAllowedCategory() const {
		switch (suggestion_type) {
		case SuggestionState::SUGGEST_TYPE_NAME:
			return PEGKeywordCategory::KEYWORD_TYPE_NAME;
		case SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME:
		case SuggestionState::SUGGEST_TABLE_FUNCTION_NAME:
			return PEGKeywordCategory::KEYWORD_TYPE_FUNC;
		default:
			return PEGKeywordCategory::KEYWORD_COL_NAME;
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
	bool IsAllowedKeyword(const string &token_text) const {
		if (!keyword_helper.IsKeyword(token_text)) {
			return true;
		}
		if (keyword_helper.KeywordCategoryType(token_text, PEGKeywordCategory::KEYWORD_UNRESERVED)) {
			return true;
		}
		return keyword_helper.KeywordCategoryType(token_text, GetAllowedCategory());
	}

	bool MatchIdentifier(MatchState &state) const {
		auto token = state.token_iterator.Current();
		if (!token) {
			return false;
		}
		auto &token_text = token->text;
		if (!IsAllowedKeyword(token_text) || !IsIdentifier(token_text)) {
			return false;
		}
		state.token_iterator.Advance();
		state.UpdateMaxTokenIndex();
		return true;
	}

	SuggestionState suggestion_type;
	const PEGKeywordHelper &keyword_helper;
};

class ReservedIdentifierMatcher : public IdentifierMatcher {
public:
	static constexpr MatcherType TYPE = MatcherType::VARIABLE;

public:
	ReservedIdentifierMatcher(SuggestionState suggestion_type, const PEGKeywordHelper &keyword_helper)
	    : IdentifierMatcher(suggestion_type, keyword_helper) {
	}

	MatcherResult MatchParseResultInternal(MatchState &state) const override {
		auto token = state.token_iterator.Current();
		if (!token) {
			return MatcherResult::Failure();
		}
		auto &token_text = token->text;
		auto start_offset = optional_idx(token->offset);
		auto token_length = optional_idx(token->length);
		if (!MatchReservedIdentifier(state)) {
			return MatcherResult::Failure();
		}
		state.token_iterator.SetPreviousTokenType(GetTokenType());
		if (!state.BuildParseResult()) {
			return MatcherResult::Success();
		}
		string result_text = token_text;
		// unlike IdentifierMatcher this rule does not unwrap path literals, it only has to avoid folding them
		const bool is_path_literal = IsSingleQuoted(result_text) && SupportsStringLiteral();
		if (IsQuoted(result_text)) {
			result_text = result_text.substr(1, result_text.size() - 2);
			result_text = StringUtil::Replace(result_text, "\"\"", "\"");
		} else if (!is_path_literal) {
			state.FoldIdentifier(result_text);
		}
		return state.AllocateParseResult<IdentifierParseResult>(result_text, start_offset, token_length);
	}

private:
	bool MatchReservedIdentifier(MatchState &state) const {
		auto token = state.token_iterator.Current();
		if (!token) {
			return false;
		}
		auto &token_text = token->text;
		if (!IsIdentifier(token_text)) {
			return false;
		}
		state.token_iterator.Advance();
		state.UpdateMaxTokenIndex();
		return true;
	}
};

} // namespace duckdb
