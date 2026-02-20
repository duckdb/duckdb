//===----------------------------------------------------------------------===//
//                         DuckDB
//
// include/parser/tokenizer/tokenizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "keyword_helper.hpp"
#include "matcher.hpp"

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
	bool TokenizeInput();

	virtual void PushToken(idx_t start, idx_t end, TokenType type, bool unterminated = false);
	virtual void OnStatementEnd(idx_t pos);
	virtual void OnLastToken(TokenizeState state, string last_word, idx_t last_pos);

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
