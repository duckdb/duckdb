#pragma once
#include "base_tokenizer.hpp"

namespace duckdb {
struct MatcherToken;

class HighlightTokenizer : public BaseTokenizer {
public:
	HighlightTokenizer(const string &sql);
	~HighlightTokenizer() override = default;

	void OnStatementEnd(idx_t pos) override;
	void OnLastToken(TokenType type, string last_word, idx_t last_pos) override;

	vector<MatcherToken> tokens;
};

} // namespace duckdb
