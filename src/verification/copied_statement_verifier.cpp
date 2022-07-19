#include "duckdb/verification/copied_statement_verifier.hpp"

namespace duckdb {

CopiedStatementVerifier::CopiedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier("Copied", move(statement_p)) {
}

StatementVerifier CopiedStatementVerifier::Create(const SQLStatement &statement) {
	return CopiedStatementVerifier(statement.Copy());
}

} // namespace duckdb
