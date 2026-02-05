#include "parser/tokenizer/highlight_tokenizer.hpp"

namespace duckdb {

HighlightTokenizer::HighlightTokenizer(const string &sql) : BaseTokenizer(sql, tokens) {
}

void HighlightTokenizer::PushToken(idx_t start, idx_t end, TokenType type, bool unterminated) {
	if (start >= end) {
		return;
	}
	string last_token = sql.substr(start, end - start);
	tokens.emplace_back(std::move(last_token), start, type, unterminated);
}

void HighlightTokenizer::OnStatementEnd(idx_t pos) {
	tokens.emplace_back(";", pos, TokenType::TERMINATOR);
}
} // namespace duckdb
