#include "parser/tokenizer/parser_tokenizer.hpp"

namespace duckdb {

ParserTokenizer::ParserTokenizer(const string &sql, vector<MatcherToken> &tokens) : BaseTokenizer(sql, tokens) {
}

void ParserTokenizer::OnStatementEnd(idx_t pos) {
	statements.push_back(std::move(tokens));
	tokens.clear();
}

void ParserTokenizer::OnLastToken(TokenType type, string last_word, idx_t last_pos) {
	if (last_word.empty()) {
		return;
	}
	tokens.emplace_back(std::move(last_word), last_pos, TokenType::IDENTIFIER);
}

} // namespace duckdb
