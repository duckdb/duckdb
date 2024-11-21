//===----------------------------------------------------------------------===//
//                         DuckDB
//
// matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

enum class SuggestionState : uint8_t {
	SUGGEST_KEYWORD,
	SUGGEST_CATALOG_NAME,
	SUGGEST_SCHEMA_NAME,
	SUGGEST_TABLE_NAME,
	SUGGEST_TYPE_NAME,
	SUGGEST_COLUMN_NAME,
	SUGGEST_FILE_NAME,
	SUGGEST_VARIABLE
};

struct AutoCompleteCandidate {
	// NOLINTNEXTLINE: allow implicit conversion from string
	AutoCompleteCandidate(string candidate_p, int32_t score_bonus = 0)
	    : candidate(std::move(candidate_p)), score_bonus(score_bonus) {
	}
	// NOLINTNEXTLINE: allow implicit conversion from const char*
	AutoCompleteCandidate(const char *candidate_p, int32_t score_bonus = 0)
	    : AutoCompleteCandidate(string(candidate_p), score_bonus) {
	}

	string candidate;
	//! The higher the score bonus, the more likely this candidate will be chosen
	int32_t score_bonus;
};

enum class MatchResultType { SUCCESS, ADDED_SUGGESTION, FAIL };

enum class SuggestionType { OPTIONAL, MANDATORY };

enum class TokenType { WORD };

struct MatcherToken {
	// NOLINTNEXTLINE: allow implicit conversion from text
	MatcherToken(string text_p) : text(std::move(text_p)) {
	}

	TokenType type = TokenType::WORD;
	string text;
};

struct MatcherSuggestion {
	// NOLINTNEXTLINE: allow implicit conversion from text
	MatcherSuggestion(const string &text) : keyword(text), type(SuggestionState::SUGGEST_KEYWORD) {
	}
	// NOLINTNEXTLINE: allow implicit conversion from suggestion state
	MatcherSuggestion(SuggestionState type) : keyword(""), type(type) {
	}

	//! Literal suggestion
	AutoCompleteCandidate keyword;
	SuggestionState type;
};

struct MatchState {
	MatchState(vector<MatcherToken> &tokens, vector<MatcherSuggestion> &suggestions)
	    : tokens(tokens), suggestions(suggestions), token_index(0) {
	}
	MatchState(MatchState &state)
	    : tokens(state.tokens), suggestions(state.suggestions), token_index(state.token_index) {
	}

	vector<MatcherToken> &tokens;
	vector<MatcherSuggestion> &suggestions;
	idx_t token_index;
};

class Matcher {
public:
	virtual ~Matcher() = default;

	//! Match
	virtual MatchResultType Match(MatchState &state) = 0;
	virtual SuggestionType AddSuggestion(MatchState &state) = 0;

	static unique_ptr<Matcher> Keyword(const string &keyword);
	static unique_ptr<Matcher> List(vector<unique_ptr<Matcher>> matchers);
	static unique_ptr<Matcher> Choice(vector<unique_ptr<Matcher>> matchers);
	static unique_ptr<Matcher> Optional(unique_ptr<Matcher> matcher);
	static unique_ptr<Matcher> Repeat(unique_ptr<Matcher> matcher, unique_ptr<Matcher> final);
	static unique_ptr<Matcher> Variable();
	static unique_ptr<Matcher> CatalogName();
	static unique_ptr<Matcher> SchemaName();
	static unique_ptr<Matcher> TypeName();
	static unique_ptr<Matcher> TableName();

	static unique_ptr<Matcher> RootMatcher();
};

} // namespace duckdb
