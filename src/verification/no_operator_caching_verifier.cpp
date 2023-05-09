#include "duckdb/verification/no_operator_caching_verifier.hpp"

namespace duckdb {

NoOperatorCachingVerifier::NoOperatorCachingVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::NO_OPERATOR_CACHING, "No operator caching", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> NoOperatorCachingVerifier::Create(const SQLStatement &statement_p) {
	return make_uniq<NoOperatorCachingVerifier>(statement_p.Copy());
}

} // namespace duckdb
