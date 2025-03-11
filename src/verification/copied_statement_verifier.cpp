#include "duckdb/verification/copied_statement_verifier.hpp"

namespace duckdb {

CopiedStatementVerifier::CopiedStatementVerifier(unique_ptr<SQLStatement> statement_p,
                                                 optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters)
    : StatementVerifier(VerificationType::COPIED, "Copied", std::move(statement_p), parameters) {
}

unique_ptr<StatementVerifier>
CopiedStatementVerifier::Create(const SQLStatement &statement,
                                optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters) {
	return make_uniq<CopiedStatementVerifier>(statement.Copy(), parameters);
}

} // namespace duckdb
