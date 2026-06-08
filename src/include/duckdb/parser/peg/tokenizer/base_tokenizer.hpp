//===----------------------------------------------------------------------===//
//                         DuckDB
//
// include/parser/tokenizer/tokenizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

enum class TokenizeState {
	STANDARD = 0,
	SINGLE_LINE_COMMENT,
	MULTI_LINE_COMMENT,
	QUOTED_IDENTIFIER,
	STRING_LITERAL,
	KEYWORD,
	NUMERIC,
	OPERATOR,
	DOLLAR_QUOTED_STRING
};

class BaseTokenizer {
public:
	BaseTokenizer(const string &sql, vector<MatcherToken> &tokens);
	virtual ~BaseTokenizer() = default;

public:
	void TokenizeInput();

	//! True iff `TokenizeInput()` finished in a state where autocomplete could be offered (the
	//! input wasn't truncated mid-comment / mid-dollar-quoted-string). Derived by inspecting the
	//! trailing sentinel: the clean-exit paths append `GetTerminator()` (which becomes
	//! `END_OF_INPUT_AUTOCOMPLETE` for autocomplete tokenizers); the dirty-exit paths always append
	//! `END_OF_INPUT`.
	bool CanAutocomplete() const;

private:
	//! Core tokenization loop. Returns true on a clean exit, false if the input ended inside an
	//! unterminated comment / dollar-quoted string. Does NOT append the trailing sentinel —
	//! `TokenizeInput()` is the one that appends `GetTerminator()` (clean) or `END_OF_INPUT`
	//! (dirty) based on the return value.
	bool TokenizeInputInternal();

public:
	virtual void PushToken(idx_t start, idx_t end, TokenType type, bool unterminated = false);
	virtual void OnStatementEnd(idx_t pos);
	virtual void OnLastToken(TokenizeState state, string last_word, idx_t last_pos);

	//! Sentinel appended at the end of the token vector on a clean exit. Override to return
	//! `END_OF_INPUT_AUTOCOMPLETE` in autocomplete tokenizers. Dirty exits (unterminated comment /
	//! dollar-quote) always append `END_OF_INPUT` regardless of this hook.
	virtual TokenType GetTerminator() const {
		return TokenType::END_OF_INPUT;
	}

	bool IsSpecialOperator(idx_t pos, idx_t &op_len) const;
	static bool IsSingleByteOperator(char c);
	static bool CharacterIsInitialNumber(char c);
	static bool CharacterIsNumber(char c);
	static bool CharacterIsScientific(char c);
	static bool CharacterIsControlFlow(char c);
	static bool CharacterIsKeyword(char c);
	static bool CharacterIsOperator(char c);
	static bool CharacterIsSpecialStringCharacter(char c);
	bool IsValidDollarTagCharacter(char c);
	TokenType TokenizeStateToType(TokenizeState state);
	static bool IsUnterminatedState(TokenizeState state);

protected:
	const string &sql;
	vector<MatcherToken> &tokens;
	PEGKeywordHelper keyword_helper;
};

} // namespace duckdb
