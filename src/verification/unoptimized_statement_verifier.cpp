#include "duckdb/verification/unoptimized_statement_verifier.hpp"

namespace duckdb {

UnoptimizedStatementVerifier::UnoptimizedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::UNOPTIMIZED, "Unoptimized", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> UnoptimizedStatementVerifier::Create(const SQLStatement &statement_p) {
	return make_uniq<UnoptimizedStatementVerifier>(statement_p.Copy());
}

} // namespace duckdb
