#pragma once
#include "base_tokenizer.hpp"

namespace duckdb {
struct MatcherToken;

class HighlightTokenizer : public BaseTokenizer {
public:
	explicit HighlightTokenizer(const string &sql);
	~HighlightTokenizer() override = default;

	void PushToken(idx_t start, idx_t end, TokenType type) override;
	void OnStatementEnd(idx_t pos) override;

	vector<MatcherToken> tokens;
};

} // namespace duckdb
