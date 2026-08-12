#pragma once
#include "duckdb/parser/peg/tokenizer/base_tokenizer.hpp"

namespace duckdb {
struct MatcherToken;

class HighlightTokenizer : public BaseTokenizer {
public:
	explicit HighlightTokenizer(const string &sql, optional_ptr<const KeywordExtension> keyword_extension = nullptr);
	~HighlightTokenizer() override = default;

	void PushToken(idx_t start, idx_t end, TokenType type, bool unterminated) override;
	void OnStatementEnd(idx_t pos) override;

	vector<MatcherToken> tokens;
};

} // namespace duckdb
