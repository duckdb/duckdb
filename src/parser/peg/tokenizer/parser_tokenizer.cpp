#include "duckdb/parser/peg/tokenizer/parser_tokenizer.hpp"
#include "duckdb/common/exception/parser_exception.hpp"

namespace duckdb {

static bool IsEmptyQuotedIdentifier(const string &sql, idx_t start, idx_t end, TokenType type) {
	return type == TokenType::IDENTIFIER && end == start + 2 && sql.substr(start, 2) == "\"\"";
}

ParserTokenizer::ParserTokenizer(const string &sql, vector<MatcherToken> &tokens) : BaseTokenizer(sql, tokens) {
}

void ParserTokenizer::PushToken(idx_t start, idx_t end, TokenType type, bool unterminated) {
	if (IsEmptyQuotedIdentifier(sql, start, end, type)) {
		throw ParserException::SyntaxError(sql, "zero-length delimited identifier", optional_idx(start));
	}
	BaseTokenizer::PushToken(start, end, type, unterminated);
}

void ParserTokenizer::OnStatementEnd(idx_t pos) {
	// Always emit ';' as a TERMINATOR token so the grammar can consume it.
	// Statement boundaries are determined by the PEG grammar (Program rule), not the tokenizer.
	tokens.emplace_back(";", pos, TokenType::TERMINATOR);
}

void ParserTokenizer::OnLastToken(TokenizeState state, string last_word, idx_t last_pos) {
	switch (state) {
	case TokenizeState::STRING_LITERAL:
		throw ParserException::SyntaxError(sql, "unterminated string literal", optional_idx(last_pos));
	case TokenizeState::QUOTED_IDENTIFIER:
		throw ParserException::SyntaxError(sql, "unterminated quoted identifier", optional_idx(last_pos));
	default:
		break;
	}
	BaseTokenizer::OnLastToken(state, std::move(last_word), last_pos);
}

} // namespace duckdb
