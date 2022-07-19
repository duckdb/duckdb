#include "duckdb/verification/prepared_statement_verifier.hpp"

namespace duckdb {

PreparedStatementVerifier::PreparedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier("Prepared", move(statement_p)) {
}

StatementVerifier PreparedStatementVerifier::Create(const SQLStatement &statement) {
	return PreparedStatementVerifier(statement.Copy());
}

} // namespace duckdb
