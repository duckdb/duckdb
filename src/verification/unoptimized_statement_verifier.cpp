#include "duckdb/verification/unoptimized_statement_verifier.hpp"

namespace duckdb {

UnoptimizedStatementVerifier::UnoptimizedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier("Unoptimized", move(statement_p)) {
}

StatementVerifier UnoptimizedStatementVerifier::Create(const SQLStatement &statement_p) {
	return UnoptimizedStatementVerifier(statement_p.Copy());
}

} // namespace duckdb
