//===----------------------------------------------------------------------===//
//                         DuckDB
//
// matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/enums/identifier_case_mode.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/token_iterator.hpp"
#include "duckdb/parser/peg/parser_packrat.hpp"
#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"

namespace duckdb {
class ClientContext;
class PEGTransformerFactory;
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
	// NOLINTNEXTLINE: allow implicit conversion from Identifier
	AutoCompleteCandidate(const Identifier &candidate_p, SuggestionState suggestion_type, int32_t score_bonus = 0,
	                      CandidateType candidate_type = CandidateType::IDENTIFIER)
	    : AutoCompleteCandidate(candidate_p.GetIdentifierName(), suggestion_type, score_bonus, candidate_type) {
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

enum class SuggestionType { OPTIONAL, MANDATORY };

enum class MatchMode : uint8_t { BUILD_PARSE_RESULT, RECOGNIZE_ONLY };

class MatcherResult {
public:
	static MatcherResult Success(optional_ptr<ParseResult> parse_result = nullptr) {
		return MatcherResult(true, parse_result);
	}

	static MatcherResult Failure() {
		return MatcherResult(false, nullptr);
	}

	bool IsSuccess() const {
		return success;
	}

	bool HasParseResult() const {
		return parse_result != nullptr;
	}

	optional_ptr<ParseResult> GetParseResult() const {
		return parse_result;
	}

private:
	MatcherResult(bool success_p, optional_ptr<ParseResult> parse_result_p)
	    : success(success_p), parse_result(parse_result_p) {
	}

private:
	bool success;
	optional_ptr<ParseResult> parse_result;
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
	MatchState(TokenIterator &token_iterator_p, vector<MatcherSuggestion> &suggestions, ParseResultAllocator &allocator,
	           idx_t &max_token_index, MatchMode mode_p = MatchMode::BUILD_PARSE_RESULT,
	           IdentifierCaseMode identifier_case_mode_p = IdentifierCaseMode::PRESERVE_CASE,
	           bool use_heap_based_parser_p = false, ParserPackratCache *packrat_cache_p = nullptr)
	    : token_iterator(token_iterator_p), suggestions(suggestions), allocator(allocator),
	      max_token_index(max_token_index), identifier_case_mode(identifier_case_mode_p),
	      packrat_cache(packrat_cache_p), mode(mode_p), use_heap_based_parser(use_heap_based_parser_p) {
	}
	MatchState(MatchState &state)
	    : token_iterator(state.token_iterator), suggestions(state.suggestions), allocator(state.allocator),
	      max_token_index(state.max_token_index), identifier_case_mode(state.identifier_case_mode),
	      packrat_cache(state.packrat_cache), mode(state.mode), use_heap_based_parser(state.use_heap_based_parser),
	      rule(state.rule) {
	}

	TokenIterator token_iterator;
	vector<MatcherSuggestion> &suggestions;
	reference_set_t<const Matcher> added_suggestions;
	ParseResultAllocator &allocator;
	idx_t &max_token_index;
	IdentifierCaseMode identifier_case_mode = IdentifierCaseMode::PRESERVE_CASE;
	ParserPackratCache *packrat_cache;
	MatchMode mode;
	bool use_heap_based_parser;
	optional_ptr<const CompiledGrammarRule> rule;

	bool BuildParseResult() const {
		return mode == MatchMode::BUILD_PARSE_RESULT;
	}

	template <class RESULT, class... ARGS>
	MatcherResult AllocateParseResult(ARGS &&... args);

	void UpdateMaxTokenIndex() {
		if (token_iterator.Position() > max_token_index) {
			max_token_index = token_iterator.Position();
		}
	}

	idx_t GetMaxTokenIndex() const {
		return max_token_index;
	}

	//! Fold a non-quoted identifier in-place according to the configured case mode
	void FoldIdentifier(string &text) const {
		switch (identifier_case_mode) {
		case IdentifierCaseMode::LOWERCASE:
			text = StringUtil::Lower(text);
			break;
		case IdentifierCaseMode::UPPERCASE:
			text = StringUtil::Upper(text);
			break;
		default:
			break;
		}
	}

	void AddSuggestion(MatcherSuggestion suggestion);
};

enum class MatcherType {
	KEYWORD,
	LIST,
	OPTIONAL,
	CHOICE,
	REPEAT,
	VARIABLE,
	STRING_LITERAL,
	NUMBER_LITERAL,
	OPERATOR,
	END_OF_INPUT
};

class Matcher {
public:
	explicit Matcher(MatcherType type) : type(type) {
	}
	virtual ~Matcher() = default;

	//! Match and construct the parse result
	MatcherResult MatchParseResult(MatchState &state) const;
	virtual MatcherResult MatchParseResultInternal(MatchState &state) const = 0;
	virtual SuggestionType AddSuggestion(MatchState &state) const;
	virtual SuggestionType AddSuggestionInternal(MatchState &state) const = 0;
	virtual string ToString() const = 0;
	void Print() const;

	MatcherType Type() const {
		return type;
	}
	void SetName(string name_p) {
		name = std::move(name_p);
	}
	void SetRule(const CompiledGrammarRule &rule_p) {
		rule = rule_p;
		name = rule_p.name;
	}
	optional_ptr<const CompiledGrammarRule> GetRule() const {
		return rule;
	}
	bool HasName() const {
		return !name.empty();
	}
	string GetName() const;
	optional_idx GetPackratId() const {
		return packrat_id;
	}
	void SetPackratMemoized() {
		packrat_memoized = true;
	}
	bool IsPackratMemoized() const {
		return packrat_memoized;
	}

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
	friend class MatcherAllocator;
	MatcherType type;
	string name;
	optional_idx packrat_id;
	bool packrat_memoized = false;
	optional_ptr<const CompiledGrammarRule> rule;
};

class KeywordInfo {
public:
	KeywordInfo() {
	}
	explicit KeywordInfo(int32_t score_bonus, char extra_char = ' ')
	    : score_bonus(score_bonus), extra_char(extra_char) {
	}

public:
	int32_t score_bonus = 0;
	char extra_char = '\0';
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

template <class RESULT, class... ARGS>
MatcherResult MatchState::AllocateParseResult(ARGS &&... args) {
	if (!BuildParseResult()) {
		return MatcherResult::Success();
	}
	auto result = allocator.Allocate(make_uniq<RESULT>(std::forward<ARGS>(args)...));
	if (rule) {
		result->SetRule(*rule);
		result->name = rule->name;
	}
	return MatcherResult::Success(result);
}

} // namespace duckdb
