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
#include "duckdb/parser/peg/keyword_table.hpp"
#include "duckdb/storage/arena_allocator.hpp"

#include <new>

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
	MatchState(const MatchState &state)
	    : token_iterator(state.token_iterator), suggestions(state.suggestions), allocator(state.allocator),
	      max_token_index(state.max_token_index), identifier_case_mode(state.identifier_case_mode),
	      packrat_cache(state.packrat_cache), mode(state.mode), use_heap_based_parser(state.use_heap_based_parser),
	      rule(state.rule) {
	}

	TokenIterator token_iterator;
	vector<MatcherSuggestion> &suggestions;
	//! The matchers that already contributed a suggestion through this state, created on first use
	unique_ptr<reference_set_t<const Matcher>> added_suggestions;
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

//! Describes which tokens can begin a successful match of a matcher. Computed once per compiled grammar by the
//! MatcherFactory and used to skip matchers that cannot possibly match the current token (see Matcher::CanStartAt).
struct MatcherFirstSet {
	//! Whether the matcher can succeed without consuming any token
	bool nullable = false;
	//! Whether the matcher can begin with a token that is not a grammar literal (identifier, string, number, ...)
	bool any_token = false;
	//! Bitmap over KeywordTable ids of the literals the matcher can begin with
	unsafe_vector<uint64_t> keywords;
	//! Whether the set is restrictive, i.e. a match must begin with one of the literals in `keywords`
	bool prunable = false;

	void AddKeyword(idx_t keyword_id) {
		auto word = keyword_id / 64;
		if (word >= keywords.size()) {
			keywords.resize(word + 1, 0);
		}
		keywords[word] |= uint64_t(1) << (keyword_id % 64);
	}
	bool HasKeyword(idx_t keyword_id) const {
		auto word = keyword_id / 64;
		return word < keywords.size() && (keywords[word] >> (keyword_id % 64)) & 1;
	}
	//! Merge the tokens another matcher can begin with (does not touch nullable)
	void MergeStart(const MatcherFirstSet &other) {
		any_token = any_token || other.any_token;
		if (other.keywords.size() > keywords.size()) {
			keywords.resize(other.keywords.size(), 0);
		}
		for (idx_t i = 0; i < other.keywords.size(); i++) {
			keywords[i] |= other.keywords[i];
		}
	}
	bool operator==(const MatcherFirstSet &other) const {
		return nullable == other.nullable && any_token == other.any_token && keywords == other.keywords;
	}
	//! Called once the set is complete: only a restrictive set needs to keep its bitmap
	void Finalize() {
		prunable = !nullable && !any_token;
		if (!prunable) {
			keywords.clear();
		}
	}
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

	//! Whether a match can begin at the current token according to the first set. A matcher that cannot start here
	//! would fail without consuming input, so skipping it changes neither the outcome, the error position nor the
	//! suggestions; at the autocomplete cursor every matcher runs.
	bool CanStartAt(MatchState &state) const {
		if (!first_set.prunable) {
			return true;
		}
		auto token = state.token_iterator.Current();
		if (!token || token->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
			return true;
		}
		auto keyword_id = state.token_iterator.CurrentKeywordId(*keyword_table);
		return keyword_id != DConstants::INVALID_INDEX && first_set.HasKeyword(keyword_id);
	}

	//! Match and construct the parse result
	MatcherResult MatchParseResult(MatchState &state) const {
		if (!CanStartAt(state)) {
			return MatcherResult::Failure();
		}
		state.rule = rule;
		if (state.use_heap_based_parser) {
			return MatchHeapBased(state);
		}
		if (packrat_memoized && state.packrat_cache) {
			return MatchMemoized(state);
		}
		return MatchParseResultInternal(state);
	}
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
	//! Marks the matcher as packrat-memoized; `slot` is its column in the ParserPackratCache
	void SetPackratMemoized(idx_t slot) {
		packrat_memoized = true;
		packrat_slot = slot;
	}
	bool IsPackratMemoized() const {
		return packrat_memoized;
	}
	void SetFirstSet(MatcherFirstSet first_set_p) {
		first_set = std::move(first_set_p);
	}
	idx_t GetPackratSlot() const {
		return packrat_slot;
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

private:
	//! Runs the matcher on the heap-based matcher stack instead of the native stack
	MatcherResult MatchHeapBased(MatchState &state) const;
	//! Runs the matcher through the packrat cache, memoizing the result per token position
	MatcherResult MatchMemoized(MatchState &state) const;

protected:
	friend class MatcherAllocator;
	MatcherType type;
	string name;
	optional_idx packrat_id;
	bool packrat_memoized = false;
	idx_t packrat_slot = 0;
	optional_ptr<const CompiledGrammarRule> rule;
	//! The tokens a match can begin with, computed once the grammar is fully constructed
	MatcherFirstSet first_set;
	//! The literals of the grammar this matcher belongs to, set by the MatcherAllocator
	optional_ptr<const KeywordTable> keyword_table;
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

	//! All matchers allocated so far, indexed by their packrat id
	const vector<unique_ptr<Matcher>> &GetMatchers() const {
		return matchers;
	}

private:
	vector<unique_ptr<Matcher>> matchers;
	//! The literals of the grammar; owned here because the matchers reference it
	KeywordTable keyword_table;
};

//! Owns the parse results of a single parse; they are placed in an arena and released together
class ParseResultAllocator {
public:
	ParseResultAllocator();
	~ParseResultAllocator();

	template <class RESULT, class... ARGS>
	RESULT &Allocate(ARGS &&... args) {
		auto result = arena.Make<RESULT>(std::forward<ARGS>(args)...);
		parse_results.push_back(*result);
		return *result;
	}

	//! The children of a sequence or repetition are gathered on a scratch stack while it matches. Matchers nest, so
	//! the stack is strictly LIFO; once the composite succeeds its children are copied into the arena.
	idx_t ChildrenBegin() const {
		return child_scratch.size();
	}
	void PushChild(ParseResult &child) {
		child_scratch.push_back(child);
	}
	void DiscardChildren(idx_t begin) {
		while (child_scratch.size() > begin) {
			child_scratch.pop_back();
		}
	}
	//! Moves the children pushed since `begin` into the arena and returns them (nullptr if there are none)
	reference<ParseResult> *TakeChildren(idx_t begin, idx_t &count);

private:
	ArenaAllocator arena;
	unsafe_vector<reference<ParseResult>> parse_results;
	unsafe_vector<reference<ParseResult>> child_scratch;
};

template <class RESULT, class... ARGS>
MatcherResult MatchState::AllocateParseResult(ARGS &&... args) {
	if (!BuildParseResult()) {
		return MatcherResult::Success();
	}
	auto &result = allocator.Allocate<RESULT>(std::forward<ARGS>(args)...);
	if (rule) {
		result.SetRule(*rule);
		result.name = rule->name;
	}
	return MatcherResult::Success(result);
}

} // namespace duckdb
