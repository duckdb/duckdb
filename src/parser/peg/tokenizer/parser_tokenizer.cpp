#include "parser/tokenizer/parser_tokenizer.hpp"

namespace duckdb {

ParserTokenizer::ParserTokenizer(const string &sql, vector<MatcherToken> &tokens) : BaseTokenizer(sql, tokens) {
}

void ParserTokenizer::OnStatementEnd(idx_t pos) {
	statements.push_back(std::move(tokens));
	tokens.clear();
}
} // namespace duckdb
