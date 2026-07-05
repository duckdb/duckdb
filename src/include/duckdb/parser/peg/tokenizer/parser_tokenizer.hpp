#pragma once
#include "duckdb/parser/peg/tokenizer/base_tokenizer.hpp"

namespace duckdb {
struct MatcherToken;

class ParserTokenizer : public BaseTokenizer {
public:
	ParserTokenizer(const string &sql, vector<MatcherToken> &tokens);
	~ParserTokenizer() override = default;

	void PushToken(idx_t start, idx_t end, TokenType type, bool unterminated = false) override;
	void OnStatementEnd(idx_t pos) override;
	void OnLastToken(TokenizeState state, string last_word, idx_t last_pos) override;
};

} // namespace duckdb
