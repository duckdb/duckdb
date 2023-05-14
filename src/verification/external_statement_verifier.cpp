#include "duckdb/verification/external_statement_verifier.hpp"

namespace duckdb {

ExternalStatementVerifier::ExternalStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::EXTERNAL, "External", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> ExternalStatementVerifier::Create(const SQLStatement &statement) {
	return make_uniq<ExternalStatementVerifier>(statement.Copy());
}

} // namespace duckdb
