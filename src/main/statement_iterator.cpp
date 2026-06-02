#include "duckdb/main/statement_iterator.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/tokenizer/parser_tokenizer.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

StatementIterator::StatementIterator(string sql_p) : sql(std::move(sql_p)) {
}

StatementIterator::~StatementIterator() = default;

StatementIterator::StatementIterator(StatementIterator &&) noexcept = default;
StatementIterator &StatementIterator::operator=(StatementIterator &&) noexcept = default;

bool StatementIterator::Peek(ClientContext &context) {
	// Already buffered from a prior Peek — just report it.
	if (current_statement) {
		return true;
	}
	if (exhausted) {
		return false;
	}
	if (!parser) {
		parser = make_uniq<Parser>(context.GetParserOptions());
	}
	if (!tokens) {
		// Tokenize the full input once. Subsequent Peek calls walk through `tokens` via
		// `token_cursor`; we never re-tokenize.
		tokens = make_uniq<vector<MatcherToken>>();
		ParserTokenizer tokenizer(sql, *tokens);
		tokenizer.TokenizeInput();
	}
	// Walk the token cursor through the cached `tokens`, calling Parser::ParseTopLevelStatement
	// repeatedly. A nullptr return with cursor advanced means a separator-only TopLevelStatement
	// (e.g. between statements or trailing ';'s); we loop past it. A nullptr return with cursor
	// at end means the input is exhausted.
	auto at_end_of_real_tokens = [&]() {
		return token_cursor >= tokens->size() || (*tokens)[token_cursor].type == TokenType::END_OF_INPUT;
	};
	while (true) {
		if (at_end_of_real_tokens()) {
			exhausted = true;
			return false;
		}
		auto stmt = parser->ParseTopLevelStatement(*tokens, token_cursor);
		if (stmt) {
			// ParseTopLevelStatement doesn't populate stmt->query (it operates on tokens, not the
			// source string). Mirror Parser::ParseQuery's per-statement post-processing here so
			// downstream consumers (error reporting, EXPLAIN, etc.) see the same shape.
			idx_t stmt_loc = stmt->stmt_location;
			idx_t stmt_len = stmt->stmt_length;
			stmt->query = sql.substr(stmt_loc, stmt_len);
			stmt->stmt_location = 0;
			stmt->stmt_length = stmt->query.size();
			if (stmt->type == StatementType::CREATE_STATEMENT) {
				auto &create = stmt->Cast<CreateStatement>();
				create.info->sql = stmt->query;
			}
			current_statement = std::move(stmt);
			return true;
		}
		if (at_end_of_real_tokens()) {
			exhausted = true;
			return false;
		}
		// separator-only TLS in the middle of the input — loop and try the next.
	}
}

unique_ptr<SQLStatement> StatementIterator::GetStatement() {
	if (!current_statement) {
		return nullptr;
	}
	return std::move(current_statement);
}

} // namespace duckdb
