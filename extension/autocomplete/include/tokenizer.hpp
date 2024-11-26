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

class BaseTokenizer {
public:
	BaseTokenizer(const string &sql, vector<MatcherToken> &tokens);
	virtual ~BaseTokenizer() = default;

public:
	void PushToken(idx_t start, idx_t end);

	bool TokenizeInput();

	virtual void OnStatementEnd(idx_t pos);
	virtual void OnLastToken(string last_word, idx_t last_pos) = 0;

	bool IsSpecialOperator(idx_t pos, idx_t &op_len) const;
	static bool IsSingleByteOperator(char c);
	static bool CharacterIsInitialNumber(char c);
	static bool CharacterIsNumber(char c);
	static bool CharacterIsControlFlow(char c);
	static bool CharacterIsKeyword(char c);
	static bool CharacterIsOperator(char c);

protected:
	const string &sql;
	vector<MatcherToken> &tokens;
};

} // namespace duckdb
