//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/tokenizer/tokenizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/once_flag.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/token_type.hpp"
#include "duckdb/parser/peg/matcher_token.hpp"

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

class Tokenizer;

//! The first check of the STANDARD state that applies to a byte, in the order the checks are made
enum class StandardByteClass : uint8_t {
	STRING_QUOTE,
	QUOTED_IDENTIFIER,
	SEMICOLON,
	DOLLAR,
	MINUS,
	SLASH,
	SPACE,
	COLON,
	SINGLE_BYTE_OPERATOR,
	NUMBER,
	SPECIAL_STRING_PREFIX,
	OPERATOR,
	WORD
};

class TokenizerBehavior {
public:
	explicit TokenizerBehavior(const string &sql);
	virtual ~TokenizerBehavior() = default;

public:
	virtual void PushToken(idx_t start, idx_t end, TokenType type, bool unterminated = false);
	//! Push a keyword or an identifier that the tokenizer already looked up in the literal table
	virtual void PushWordToken(idx_t start, idx_t end, TokenType type, const GrammarLiteralTable &table,
	                           LiteralInfo info);
	virtual void OnStatementEnd(idx_t pos);
	virtual void OnLastToken(const Tokenizer &tokenizer, TokenizeState state, string last_word, idx_t last_pos);

	//! Sentinel appended at the end of the token vector on a clean exit. Override to return
	//! `END_OF_INPUT_AUTOCOMPLETE` for autocomplete behavior. Dirty exits (unterminated comment /
	//! dollar-quote) always append `END_OF_INPUT` regardless of this hook.
	virtual TokenType GetTerminator() const {
		return TokenType::END_OF_INPUT;
	}

protected:
	//! Append the token of a non-empty input range, with what precedes it
	MatcherToken &EmplaceToken(idx_t start, idx_t end, TokenType type, bool unterminated = false) {
		D_ASSERT(start < end);
		tokens.emplace_back(sql.data() + start, end - start, start, type, unterminated);
		auto &token = tokens.back();
		MarkPrecedingTrivia(token);
		return token;
	}

private:
	//! Record on the token that was just pushed whether a newline or a block comment precedes it
	void MarkPrecedingTrivia(MatcherToken &token) {
		if (tokens.size() < 2) {
			return;
		}
		auto &previous_token = tokens[tokens.size() - 2];
		auto previous_token_end = previous_token.offset + previous_token.length;
		if (has_block_comment && last_block_comment_position >= previous_token_end &&
		    last_block_comment_position < token.offset) {
			token.preceded_by_block_comment = true;
		}
		for (idx_t pos = previous_token_end; pos < token.offset; pos++) {
			if (StringUtil::CharacterIsNewline(sql[pos])) {
				token.preceded_by_newline = true;
				break;
			}
		}
	}

public:
	const string &sql;
	vector<MatcherToken> tokens;
	bool has_block_comment = false;
	idx_t last_block_comment_position = 0;
};

class Tokenizer {
public:
	virtual ~Tokenizer() = default;
	explicit Tokenizer(const PEGKeywordHelper &keyword_helper);

public:
	//! Tokenize the behavior's input and return whether autocomplete can be offered.
	virtual bool TokenizeInput(TokenizerBehavior &behavior) const;

protected:
	virtual bool BackslashEscapesStringLiterals() const;
	virtual bool IsQuotedIdentifierDelimiter(char character) const;
	virtual void PushOperatorToken(TokenizerBehavior &behavior, idx_t start, idx_t end) const;
	void PushWordToken(TokenizerBehavior &behavior, idx_t start, idx_t end) const;
	virtual void HandleLastToken(TokenizerBehavior &behavior, TokenizeState state, const string &sql,
	                             idx_t last_pos) const;

private:
	//! Core tokenization loop. Returns true on a clean exit, false if the input ended inside an
	//! unterminated comment / dollar-quoted string. Does NOT append the trailing sentinel —
	//! `TokenizeInput()` is the one that appends `GetTerminator()` (clean) or `END_OF_INPUT`
	//! (dirty) based on the return value.
	bool TokenizeInputInternal(TokenizerBehavior &behavior) const;
	bool IsCompoundColonToken(const string &sql, idx_t pos, idx_t &token_length) const;
	//! Scan the word starting at position i and push it, or enter the KEYWORD state when the input ends in it
	void TokenizeWord(TokenizerBehavior &behavior, const bool *is_word, idx_t &i, idx_t &last_pos,
	                  TokenizeState &state) const;

	//! What the STANDARD state knows about each byte
	struct ByteTables {
		array<StandardByteClass, 256> standard_classes;
		//! The token class of the operator token that consists of the byte alone
		array<uint8_t, 256> operator_token_classes;
	};
	//! Built on first use, since the quoted identifier delimiters are a virtual property of the tokenizer
	const ByteTables &GetByteTables() const;
	//! Push an operator token of one byte with its literal info
	void PushSingleByteOperator(TokenizerBehavior &behavior, const ByteTables &byte_tables,
	                            const GrammarLiteralTable &table, idx_t byte_position, idx_t offset) const;

public:
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

public:
	const PEGKeywordHelper &keyword_helper;

private:
	mutable OnceFlag byte_tables_once;
	mutable ByteTables byte_tables;
};

} // namespace duckdb
