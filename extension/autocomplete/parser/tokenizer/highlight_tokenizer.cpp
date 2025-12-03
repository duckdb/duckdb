#include "parser/tokenizer/highlight_tokenizer.hpp"

namespace duckdb {

HighlightTokenizer::HighlightTokenizer(const string &sql) : BaseTokenizer(sql, tokens) {
}

void HighlightTokenizer::OnStatementEnd(idx_t pos) {
}
void HighlightTokenizer::OnLastToken(TokenType type, string last_word, idx_t last_pos) {
	tokens.emplace_back(std::move(last_word), last_pos, TokenType::IDENTIFIER);
}
} // namespace duckdb
