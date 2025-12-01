#pragma once
#include "base_tokenizer.hpp"

namespace duckdb {
struct MatcherToken;

class ParserTokenizer : public BaseTokenizer {
public:
	ParserTokenizer(const string &sql, vector<MatcherToken> &tokens);
	~ParserTokenizer() override = default;

	void OnStatementEnd(idx_t pos) override;
	void OnLastToken(TokenizeState state, string last_word, idx_t last_pos) override;

	vector<vector<MatcherToken>> statements;
};

} // namespace duckdb