#include "parser/tokenizer/parser_tokenizer.hpp"

namespace duckdb {

// Constructor Implementation
ParserTokenizer::ParserTokenizer(const string &sql, vector<MatcherToken> &tokens)
	: BaseTokenizer(sql, tokens) {
}

// Method Implementation: OnStatementEnd
void ParserTokenizer::OnStatementEnd(idx_t pos) {
	// Move the current list of tokens into the statements list
	statements.push_back(std::move(tokens));

	// Clear the current token buffer to prepare for the next statement
	tokens.clear();
}

// Method Implementation: OnLastToken
void ParserTokenizer::OnLastToken(TokenizeState state, string last_word, idx_t last_pos) {
	if (last_word.empty()) {
		return;
	}
	tokens.emplace_back(std::move(last_word), last_pos);
}

} // namespace duckdb