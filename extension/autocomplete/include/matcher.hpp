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
#include "duckdb/common/reference_map.hpp"
#include "transformer/parse_result.hpp"

namespace duckdb {
class ParseResultAllocator;
class Matcher;
class MatcherAllocator;

enum class SuggestionState : uint8_t {
	SUGGEST_KEYWORD,
	SUGGEST_CATALOG_NAME,
	SUGGEST_SCHEMA_NAME,
	SUGGEST_TABLE_NAME,
	SUGGEST_TYPE_NAME,
	SUGGEST_COLUMN_NAME,
	SUGGEST_FILE_NAME,
	SUGGEST_DIRECTORY,
	SUGGEST_VARIABLE,
	SUGGEST_SCALAR_FUNCTION_NAME,
	SUGGEST_TABLE_FUNCTION_NAME,
	SUGGEST_PRAGMA_NAME,
	SUGGEST_SETTING_NAME,
	SUGGEST_RESERVED_VARIABLE
};

enum class CandidateType { KEYWORD, IDENTIFIER, LITERAL };

struct AutoCompleteCandidate {
	// NOLINTNEXTLINE: allow implicit conversion from string
	AutoCompleteCandidate(string candidate_p, SuggestionState suggestion_type, int32_t score_bonus = 0,
	                      CandidateType candidate_type = CandidateType::IDENTIFIER)
	    : candidate(std::move(candidate_p)), suggestion_type(suggestion_type), score_bonus(score_bonus),
	      candidate_type(candidate_type) {
	}
	// NOLINTNEXTLINE: allow implicit conversion from const char*
	AutoCompleteCandidate(const char *candidate_p, SuggestionState suggestion_type, int32_t score_bonus = 0,
	                      CandidateType candidate_type = CandidateType::IDENTIFIER)
	    : AutoCompleteCandidate(string(candidate_p), suggestion_type, score_bonus, candidate_type) {
	}

	string candidate;
	//! Type being suggested
	SuggestionState suggestion_type;
	//! The higher the score bonus, the more likely this candidate will be chosen
	int32_t score_bonus;
	//! The type of candidate we are suggesting - this modifies how we handle quoting/case sensitivity
	CandidateType candidate_type;
	//! Extra char to push at the back
	char extra_char = '\0';
	//! Suggestion position
	idx_t suggestion_pos = 0;
	//! The final score
	optional_idx score;
};

struct AutoCompleteSuggestion {
	AutoCompleteSuggestion(string text_p, idx_t pos, string type_p, idx_t score, char extra_char_p)
	    : text(std::move(text_p)), pos(pos), type(std::move(type_p)), score(score), extra_char(extra_char_p) {
	}

	string text;
	idx_t pos;
	string type;
	idx_t score;
	char extra_char;
};

enum class MatchResultType { SUCCESS, FAIL };

enum class SuggestionType { OPTIONAL, MANDATORY };

enum class TokenType { WORD };

struct MatcherToken {
	// NOLINTNEXTLINE: allow implicit conversion from text
	MatcherToken(string text_p, idx_t offset_p) : text(std::move(text_p)), offset(offset_p) {
		length = text.length();
	}

	TokenType type = TokenType::WORD;
	string text;
	idx_t offset = 0;
	idx_t length = 0;
};

struct MatcherSuggestion {
	// NOLINTNEXTLINE: allow implicit conversion from auto-complete candidate
	MatcherSuggestion(AutoCompleteCandidate keyword_p) : keyword(std::move(keyword_p)), type(keyword.suggestion_type) {
	}
	// NOLINTNEXTLINE: allow implicit conversion from suggestion state
	MatcherSuggestion(SuggestionState type, char extra_char = '\0')
	    : keyword("", type), type(type), extra_char(extra_char) {
	}

	//! Literal suggestion
	AutoCompleteCandidate keyword;
	SuggestionState type;
	char extra_char = '\0';
};

struct MatchState {
	MatchState(vector<MatcherToken> &tokens, vector<MatcherSuggestion> &suggestions, ParseResultAllocator &allocator)
	    : tokens(tokens), suggestions(suggestions), token_index(0), allocator(allocator) {
	}
	MatchState(MatchState &state)
	    : tokens(state.tokens), suggestions(state.suggestions), token_index(state.token_index),
	      allocator(state.allocator) {
	}

	vector<MatcherToken> &tokens;
	vector<MatcherSuggestion> &suggestions;
	reference_set_t<const Matcher> added_suggestions;
	idx_t token_index;
	ParseResultAllocator &allocator;

	void AddSuggestion(MatcherSuggestion suggestion);
};

enum class MatcherType { KEYWORD, LIST, OPTIONAL, CHOICE, REPEAT, VARIABLE, STRING_LITERAL, NUMBER_LITERAL, OPERATOR };

class Matcher {
public:
	explicit Matcher(MatcherType type) : type(type) {
	}
	virtual ~Matcher() = default;

	//! Match
	virtual MatchResultType Match(MatchState &state) const = 0;
	virtual optional_ptr<ParseResult> MatchParseResult(MatchState &state) const = 0;
	virtual SuggestionType AddSuggestion(MatchState &state) const;
	virtual SuggestionType AddSuggestionInternal(MatchState &state) const = 0;
	virtual string ToString() const = 0;
	void Print() const;

	static Matcher &RootMatcher(MatcherAllocator &allocator);

	MatcherType Type() const {
		return type;
	}
	void SetName(string name_p) {
		name = std::move(name_p);
	}
	string GetName() const;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast matcher to type - matcher type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast matcher to type - matcher type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	MatcherType type;
	string name;
};

class MatcherAllocator {
public:
	Matcher &Allocate(unique_ptr<Matcher> matcher);

private:
	vector<unique_ptr<Matcher>> matchers;
};

class ParseResultAllocator {
public:
	optional_ptr<ParseResult> Allocate(unique_ptr<ParseResult> parse_result);

private:
	vector<unique_ptr<ParseResult>> parse_results;
};

} // namespace duckdb
