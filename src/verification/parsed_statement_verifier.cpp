#include "duckdb/verification/parsed_statement_verifier.hpp"

#include "duckdb/parser/parser.hpp"

namespace duckdb {

ParsedStatementVerifier::ParsedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::PARSED, "Parsed", move(statement_p)) {
}

unique_ptr<StatementVerifier> ParsedStatementVerifier::Create(const SQLStatement &statement) {
	auto query_str = statement.ToString();
	Parser parser;
	parser.ParseQuery(query_str);
	D_ASSERT(parser.statements.size() == 1);
	D_ASSERT(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	return make_unique<ParsedStatementVerifier>(move(parser.statements[0]));
}

} // namespace duckdb
