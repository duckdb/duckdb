#pragma once
#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"

namespace duckdb {
struct MatcherToken;

class HighlightTokenizerBehavior : public TokenizerBehavior {
public:
	HighlightTokenizerBehavior(const string &sql, vector<MatcherToken> &tokens);
	~HighlightTokenizerBehavior() override = default;

	void PushToken(idx_t start, idx_t end, TokenType type, bool unterminated) override;
	void OnStatementEnd(idx_t pos) override;
};

} // namespace duckdb
