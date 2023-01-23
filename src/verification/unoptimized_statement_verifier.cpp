#include "duckdb/verification/unoptimized_statement_verifier.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

UnoptimizedStatementVerifier::UnoptimizedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::UNOPTIMIZED, "Unoptimized", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> UnoptimizedStatementVerifier::Create(const SQLStatement &statement_p) {
	return make_unique<UnoptimizedStatementVerifier>(statement_p.Copy());
}

} // namespace duckdb
