#pragma once
#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"

namespace duckdb {
struct MatcherToken;

class ParserTokenizerBehavior : public TokenizerBehavior {
public:
	ParserTokenizerBehavior(const string &sql, vector<MatcherToken> &tokens);
	~ParserTokenizerBehavior() override = default;

	void PushToken(idx_t start, idx_t end, TokenType type, bool unterminated = false) override;
	void OnStatementEnd(idx_t pos) override;
	void OnLastToken(TokenizeState state, string last_word, idx_t last_pos) override;
};

} // namespace duckdb
