#include "duckdb/parser/peg/tokenizer/parser_tokenizer.hpp"

namespace duckdb {

ParserTokenizer::ParserTokenizer(const string &sql, vector<MatcherToken> &tokens) : BaseTokenizer(sql, tokens) {
}

void ParserTokenizer::OnStatementEnd(idx_t pos) {
	// Always emit ';' as a TERMINATOR token so the grammar can consume it.
	// Statement boundaries are determined by the PEG grammar (Program rule), not the tokenizer.
	tokens.emplace_back(";", pos, TokenType::TERMINATOR);
}

} // namespace duckdb
