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
class MatcherAllocator;

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

enum class CandidateMatchCase {
	MATCH_CASE,
	KEEP_CASE
};

struct AutoCompleteCandidate {
	// NOLINTNEXTLINE: allow implicit conversion from string
	AutoCompleteCandidate(string candidate_p, int32_t score_bonus = 0, CandidateMatchCase case_type = CandidateMatchCase::KEEP_CASE)
	    : candidate(std::move(candidate_p)), score_bonus(score_bonus), case_type(case_type) {
	}
	// NOLINTNEXTLINE: allow implicit conversion from const char*
	AutoCompleteCandidate(const char *candidate_p, int32_t score_bonus = 0, CandidateMatchCase case_type = CandidateMatchCase::KEEP_CASE)
	    : AutoCompleteCandidate(string(candidate_p), score_bonus, case_type) {
	}

	string candidate;
	//! The higher the score bonus, the more likely this candidate will be chosen
	int32_t score_bonus;
	//! Whether or not we match the case of the original type or preserve the case of the candidate
	CandidateMatchCase case_type;
	//! Extra char to push at the back
	char extra_char = '\0';
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
	MatcherSuggestion(AutoCompleteCandidate keyword_p) : keyword(std::move(keyword_p)), type(SuggestionState::SUGGEST_KEYWORD) {
	}
	// NOLINTNEXTLINE: allow implicit conversion from suggestion state
	MatcherSuggestion(SuggestionState type, char extra_char = '\0') : keyword(""), type(type), extra_char(extra_char) {
	}

	//! Literal suggestion
	AutoCompleteCandidate keyword;
	SuggestionState type;
	char extra_char = '\0';
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

	static Matcher &RootMatcher(MatcherAllocator &allocator);
};

class MatcherAllocator {
public:
	Matcher &Allocate(unique_ptr<Matcher> matcher);

private:
	vector<unique_ptr<Matcher>> matchers;
};

} // namespace duckdb
