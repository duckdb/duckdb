#include "duckdb/verification/copied_statement_verifier.hpp"

namespace duckdb {

CopiedStatementVerifier::CopiedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::COPIED, "Copied", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> CopiedStatementVerifier::Create(const SQLStatement &statement) {
	return make_uniq<CopiedStatementVerifier>(statement.Copy());
}

} // namespace duckdb
