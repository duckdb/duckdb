//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/tokenizer/tokenizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/token_type.hpp"

namespace duckdb {

struct ParserCache;

struct MatcherToken {
	// NOLINTNEXTLINE: allow implicit conversion from text
	MatcherToken(string text_p, idx_t offset_p, TokenType type_p, bool unterminated_p = false)
	    : type(type_p), text(std::move(text_p)), offset(offset_p), unterminated(unterminated_p) {
		length = text.length();
	}

	TokenType type;
	string text;
	idx_t offset = 0;
	idx_t length = 0;
	bool unterminated = false;
};

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

class Tokenizer;

class TokenizerBehavior {
public:
	TokenizerBehavior(const string &sql, vector<MatcherToken> &tokens);
	virtual ~TokenizerBehavior() = default;

public:
	virtual void PushToken(idx_t start, idx_t end, TokenType type, bool unterminated = false);
	virtual void OnStatementEnd(idx_t pos);
	virtual void OnLastToken(TokenizeState state, string last_word, idx_t last_pos);

	//! Sentinel appended at the end of the token vector on a clean exit. Override to return
	//! `END_OF_INPUT_AUTOCOMPLETE` for autocomplete behavior. Dirty exits (unterminated comment /
	//! dollar-quote) always append `END_OF_INPUT` regardless of this hook.
	virtual TokenType GetTerminator() const {
		return TokenType::END_OF_INPUT;
	}

protected:
	const string &sql;
	vector<MatcherToken> &tokens;
	optional_ptr<const PEGKeywordHelper> keyword_helper;

	friend class Tokenizer;
};

class Tokenizer {
public:
	virtual ~Tokenizer() = default;

public:
	//! Tokenize the behavior's input and return whether autocomplete can be offered.
	virtual bool TokenizeInput(TokenizerBehavior &behavior) const;

private:
	friend struct ParserCache;
	explicit Tokenizer(const PEGKeywordHelper &keyword_helper);

	//! Core tokenization loop. Returns true on a clean exit, false if the input ended inside an
	//! unterminated comment / dollar-quoted string. Does NOT append the trailing sentinel —
	//! `TokenizeInput()` is the one that appends `GetTerminator()` (clean) or `END_OF_INPUT`
	//! (dirty) based on the return value.
	bool TokenizeInputInternal(TokenizerBehavior &behavior) const;

public:
	bool IsSpecialOperator(const string &sql, idx_t pos, idx_t &op_len) const;
	static bool IsSingleByteOperator(char c);
	static bool CharacterIsInitialNumber(char c);
	static bool CharacterIsNumber(char c);
	static bool CharacterIsScientific(char c);
	static bool CharacterIsControlFlow(char c);
	static bool CharacterIsKeyword(char c);
	static bool CharacterIsOperator(char c);
	static bool CharacterIsSpecialStringCharacter(char c);
	static bool IsValidDollarTagCharacter(char c);
	static TokenType TokenizeStateToType(TokenizeState state);
	static bool IsUnterminatedState(TokenizeState state);

protected:
	reference<const PEGKeywordHelper> keyword_helper;
};

} // namespace duckdb
