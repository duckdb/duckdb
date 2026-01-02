//===----------------------------------------------------------------------===//
//                         DuckDB
//
// tokenizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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
	void PushToken(idx_t start, idx_t end);

	bool TokenizeInput();

	virtual void OnStatementEnd(idx_t pos);
	virtual void OnLastToken(TokenizeState state, string last_word, idx_t last_pos) = 0;

	bool IsSpecialOperator(idx_t pos, idx_t &op_len) const;
	static bool IsSingleByteOperator(char c);
	static bool CharacterIsInitialNumber(char c);
	static bool CharacterIsNumber(char c);
	static bool CharacterIsScientific(char c);
	static bool CharacterIsControlFlow(char c);
	static bool CharacterIsKeyword(char c);
	static bool CharacterIsOperator(char c);
	bool IsValidDollarTagCharacter(char c);

protected:
	const string &sql;
	vector<MatcherToken> &tokens;
};

} // namespace duckdb
