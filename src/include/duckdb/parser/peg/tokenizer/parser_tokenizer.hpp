#pragma once
#include "duckdb/parser/peg/tokenizer/base_tokenizer.hpp"

namespace duckdb {
struct MatcherToken;

class ParserTokenizer : public BaseTokenizer {
public:
	ParserTokenizer(const string &sql, vector<MatcherToken> &tokens);
	~ParserTokenizer() override = default;

	void OnStatementEnd(idx_t pos) override;

	vector<vector<MatcherToken>> statements;
};

} // namespace duckdb
