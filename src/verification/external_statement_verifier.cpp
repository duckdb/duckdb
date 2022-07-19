#include "duckdb/verification/external_statement_verifier.hpp"

namespace duckdb {

ExternalStatementVerifier::ExternalStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier("External", move(statement_p)) {
}

StatementVerifier ExternalStatementVerifier::Create(const SQLStatement &statement) {
	return ExternalStatementVerifier(statement.Copy());
}

} // namespace duckdb
