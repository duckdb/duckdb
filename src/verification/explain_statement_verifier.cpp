#include "duckdb/verification/explain_statement_verifier.hpp"

namespace duckdb {

ExplainStatementVerifier::ExplainStatementVerifier(unique_ptr<SQLStatement> statement_p,
                                                   optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters)
    : StatementVerifier(VerificationType::EXPLAIN, "Explain", std::move(statement_p), parameters) {
}

unique_ptr<StatementVerifier>
ExplainStatementVerifier::Create(const SQLStatement &statement,
                                 optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters) {
	return make_uniq<ExplainStatementVerifier>(statement.Copy(), parameters);
}

} // namespace duckdb
